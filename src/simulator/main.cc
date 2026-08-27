/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2026 ScyllaDB
 */

//
// A validation scenario for the simulator.
//
// The point of the simulator is to run real Seastar code -- code compiled
// against the same core headers as the reactor build -- without a reactor,
// so what validates it is real Seastar code doing something non-trivial.
// This runs `seastar_rpc`, unmodified, over the simulated network:
//
//   - four shards;
//   - shard 0 listens on an address and serves an RPC verb;
//   - shard 2 connects to it and opens an RPC stream;
//   - shard 2 sends a message on the stream and reads the response;
//   - both ends close the stream and the connection.
//
// Everything it exercises is a piece of the simulator: cross-shard calls
// (shard 2 talks to a listener registered by shard 0), the simulated network
// (connect, accept, read, write, shutdown), the run queue (every continuation
// RPC creates), and the futures underneath all of it.
//
// The scenario is written with coroutines rather than `seastar::async`.
// A Seastar thread would have to block a shard thread inside `future::get()`,
// which the engine cannot allow: only one shard runs at a time, so a blocked
// shard would stop the engine that is supposed to resolve the future.
//

#include "engine.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>
#include <seastar/net/api.hh>
#include <seastar/rpc/rpc.hh>
#include <seastar/util/log.hh>

#include <cstdio>
#include <cstdlib>
#include <exception>

using namespace seastar;

namespace {

logger test_logger("scenario");

// RPC's own logger.  It is off by default; raising its level is the first
// thing to try when the scenario stops behaving.
logger rpc_logger("rpc");

// The port shard 0 listens on.  Under simulation an address is just an entry
// in the engine's listener table, so any port will do.
constexpr uint16_t server_port = 4242;

// A serializer for the two types the scenario puts on the wire.
//
// RPC requires the serializer as a customization point; this is the smallest
// one which handles an `int32_t` and an `sstring`.
struct serializer {
};

template <typename T, typename Output>
void write_arithmetic_type(Output& out, T v) {
    static_assert(std::is_arithmetic_v<T>, "must be arithmetic type");
    out.write(reinterpret_cast<const char*>(&v), sizeof(T));
}

template <typename T, typename Input>
T read_arithmetic_type(Input& in) {
    static_assert(std::is_arithmetic_v<T>, "must be arithmetic type");
    T v;
    in.read(reinterpret_cast<char*>(&v), sizeof(T));
    return v;
}

template <typename Output>
void write(serializer, Output& output, int32_t v) { write_arithmetic_type(output, v); }
template <typename Input>
int32_t read(serializer, Input& input, rpc::type<int32_t>) { return read_arithmetic_type<int32_t>(input); }

template <typename Output>
void write(serializer, Output& out, const sstring& v) {
    write_arithmetic_type(out, uint32_t(v.size()));
    out.write(v.c_str(), v.size());
}

template <typename Input>
sstring read(serializer, Input& in, rpc::type<sstring>) {
    auto size = read_arithmetic_type<uint32_t>(in);
    sstring ret = uninitialized_string(size);
    in.read(ret.data(), size);
    return ret;
}

using test_protocol = rpc::protocol<serializer>;

// The verb the client invokes.  It takes the client's end of a stream and
// hands back the server's end, which is how RPC streams are established: the
// call carries a sink one way and returns a source the other.
constexpr uint32_t echo_verb = 1;

// Everything shard 0 -- the server -- owns.
//
// It lives for the whole run and is reached from shard 0 only, so a plain
// pointer in a `thread_local` is enough; nothing else ever touches it.
struct server_state {
    test_protocol proto{serializer()};
    std::unique_ptr<test_protocol::server> server;
    // The stream work the verb handler started, which has to finish before
    // the run is over.
    future<> stream_done = make_ready_future<>();
};

thread_local server_state* g_server = nullptr;

// Fails the scenario.  Anything unexpected is a bug in the simulator, and
// there is nothing to be gained by carrying on past it.
void check(bool ok, const char* what) {
    if (!ok) {
        test_logger.error("check failed: {}", what);
        std::abort();
    }
}

// Brings up the server on this shard.
//
// The listener is registered in the engine here, which is what makes the
// address reachable: a client connecting to it will find this entry, and
// nothing else.
future<> start_server() {
    test_logger.info("starting server on port {}", server_port);
    g_server = new server_state();
    g_server->proto.set_logger(&rpc_logger);
    g_server->proto.register_handler(echo_verb,
            [] (int32_t token, rpc::source<sstring> source) {
        test_logger.info("server: stream opened with token {}", token);
        check(token == 666, "server received the expected token");

        // The reply end of the stream.  The handler returns it, and RPC
        // delivers it to the caller as a source.
        auto sink = source.template make_sink<serializer, sstring>();

        // Read the client's messages, echo each one back, and shut the
        // stream down when the client closes its end.  Nothing is waiting
        // for this, so it is kept in `stream_done` for the scenario to wait
        // on before it finishes.
        g_server->stream_done = [] (rpc::source<sstring> source,
                rpc::sink<sstring> sink) -> future<> {
            while (auto data = co_await source()) {
                auto msg = std::get<0>(*data);
                test_logger.info("server: received {:?}, echoing it back", msg);
                co_await sink(sstring("echo: ") + msg);
                co_await sink.flush();
            }
            test_logger.info("server: client closed the stream, closing ours");
            co_await sink.close();
        }(source, sink);

        return sink;
    });

    // Streams need a streaming domain: a stream is carried by a second
    // connection, and the domain is how RPC finds the server that the first
    // connection belongs to when the stream's call arrives.
    rpc::server_options opts;
    opts.streaming_domain = rpc::streaming_domain_type(1);

    g_server->server = std::make_unique<test_protocol::server>(g_server->proto,
            opts, socket_address(uint32_t(INADDR_LOOPBACK), server_port));
    return make_ready_future<>();
}

// Shuts the server down and waits for everything it started.
future<> stop_server() {
    test_logger.info("stopping server");
    co_await std::move(g_server->stream_done);
    co_await g_server->server->shutdown();
    co_await g_server->server->stop();
    delete g_server;
    g_server = nullptr;
}

// The client half of the scenario, run on shard 2.
future<> run_client() {
    auto addr = socket_address(uint32_t(INADDR_LOOPBACK), server_port);

    test_logger.info("client: connecting to {}", addr);
    test_protocol proto(serializer{});
    proto.set_logger(&rpc_logger);
    test_protocol::client client(proto, addr);

    // Opening a stream takes a second connection to the same server, which
    // is why this needs a socket of its own.
    test_logger.info("client: opening a stream");
    auto sink = co_await client.make_stream_sink<serializer, sstring>(make_socket());

    // Invoking the verb hands the server our end of the stream and gives us
    // back its end.
    auto call = proto.make_client<rpc::source<sstring> (int32_t, rpc::sink<sstring>)>(echo_verb);
    auto source = co_await call(client, 666, sink);

    // Read the stream in the background, for as long as the server keeps it
    // open.  It has to be a separate fiber: closing the sink tears the
    // stream connection down, and a read started after that would find an
    // aborted queue rather than the end of the stream.
    auto reading = [] (rpc::source<sstring> source) -> future<> {
        auto reply = co_await source();
        check(bool(reply), "client received a reply");
        auto msg = std::get<0>(*reply);
        test_logger.info("client: received {:?}", msg);
        check(msg == "echo: hello", "the reply is the echoed message");

        // Reading on: the server echoes only what it is sent, so the next
        // thing on the stream is the end of it.
        auto eof = co_await source();
        check(!eof, "the server closed its end of the stream");
        test_logger.info("client: reached the end of the stream");
    }(source);

    test_logger.info("client: sending a message on the stream");
    co_await sink(sstring("hello"));
    co_await sink.flush();

    // Close our end of the stream.  That is what ends the server's read
    // loop, which then closes its own end, which is what `reading` above is
    // waiting to see.
    test_logger.info("client: closing the stream");
    co_await sink.close();
    co_await std::move(reading);

    test_logger.info("client: closing the connection");
    co_await client.stop();
    test_logger.info("client: done");
}

// The scenario, from shard 0.
future<> scenario() {
    test_logger.info("running on {} shards", this_smp_shard_count());

    co_await start_server();

    // The client runs on shard 2, so getting it there and getting its result
    // back is itself a cross-shard call.
    test_logger.info("handing the client half to shard 2");
    co_await smp::submit_to(2, [] {
        return run_client();
    });

    co_await stop_server();

    test_logger.info("scenario finished");
}

} // anonymous namespace

int main() {
    try {
        simulator::engine engine(4);
        engine.run(scenario);
    } catch (...) {
        std::fprintf(stderr, "scenario failed: ");
        try {
            throw;
        } catch (const std::exception& e) {
            std::fprintf(stderr, "%s\n", e.what());
        } catch (...) {
            std::fprintf(stderr, "unknown exception\n");
        }
        return 1;
    }
    return 0;
}
