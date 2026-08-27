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
//   - shards 0 and 1 both listen on one address and serve an RPC verb;
//   - shard 2 connects to it and opens an RPC stream;
//   - shard 2 sends a message on the stream and reads the response;
//   - both ends close the stream and the connection.
//
// Everything it exercises is a piece of the simulator: cross-shard calls
// (shard 2 talks to a listener registered by shards 0 and 1), the simulated
// network (listen, connect, accept, read, write, shutdown), the run queue
// (every continuation RPC creates), and the futures underneath all of it.
//
// Two listening shards rather than one is deliberate.  RPC streams are built
// to be used from a shard other than the one which owns the stream's
// connection, through `foreign_ptr` and a `batched_queue` per direction; when
// one shard owns both the connection and the stream, all of that collapses
// into direct calls and goes untested.  Listening on two shards puts the
// client's two connections -- the RPC connection and the stream's own -- on
// different server shards, which is how a real sharded server reaches that
// state.  See `run_client()` below for what it sets in motion.
//
// The scenario is written with coroutines rather than `seastar::async`.
// A Seastar thread would have to block a shard thread inside `future::get()`,
// which the engine cannot allow: only one shard runs at a time, so a blocked
// shard would stop the engine that is supposed to resolve the future.
//
//
// Running it
// ----------
//
//   ninja -C build/dev seastar_simulator_link_test
//   ./build/dev/seastar_simulator_link_test
//
// It runs the scenario once under the engine's default order -- the
// `baseline` line -- and then once per task under
// `freezing_scheduling_policy`, which sets that one task aside, runs
// everything else as far as it will go, and only then lets the frozen task
// run.  Each of those is a different interleaving of the same program.  Only
// the failures are printed, and the exit status is non-zero if there were
// any.
//
// Every run is deterministic and the whole sweep is reproducible: the engine
// makes every scheduling choice itself, so the same binary prints the same
// output every time.  A change in the numbers is a change in behaviour, not
// in the weather.
//
//
// The bugs it currently exposes
// -----------------------------
//
// The scenario passes under the default order and fails under some of the
// interleavings.  Both bugs are in this file -- in the scenario, not in the
// simulator or in RPC -- and both are left unfixed on purpose, because they
// are what demonstrates that the sweep works.
//
// 1. A stream teardown race, which the sweep reports directly:
//
//        ./build/dev/seastar_simulator_link_test
//        ...
//        freeze task 59: ... -- FAILED: rpc stream was closed by peer
//        sweep finished: 152 interleavings, 6 failed
//
//    `run_client()` reads the stream in a background fiber while the
//    foreground closes the sink.  The fiber is supposed to see the server
//    close its end first; freezing a task at the wrong moment lets
//    `sink.close()` tear the connection down before the fiber's second
//    `source()` resolves, so it gets an aborted queue instead of a clean end
//    of stream.  The comment above that fiber already notes the hazard.
//
// 2. A use-after-free in teardown, which needs valgrind to see -- it corrupts
//    memory without failing the run, and on different interleavings than the
//    ones above:
//
//        valgrind ./build/dev/seastar_simulator_link_test
//        ...
//        ==*== Invalid read of size 8
//        ==*==    at ... std::_Hashtable<seastar::rpc::connection_id, ...>::erase
//        ==*==    by ... seastar::rpc::server::connection::process
//        ==*==  Address ... is 192 bytes inside a block of size 400 free'd
//        ==*==    at ... operator delete
//        ==*==    by ... stop_server
//        ==*== ERROR SUMMARY: 25 errors from 5 contexts
//
//    `stop_server()` below does `shutdown()`, `stop()`, then `delete
//    g_server`.  On some interleavings an `rpc::server::connection::process`
//    fiber is still live at that point and reads the server's connection
//    table after it has been freed.  The baseline run is clean under
//    valgrind; only the frozen interleavings reach it.
//
// Note that the two sets of interleavings are disjoint: the runs which
// corrupt memory are not the ones which fail loudly.  That is the argument
// for running the sweep under valgrind rather than trusting the exit status.
//
//
// Sanitizers
// ----------
//
// The sanitize build links but cannot run this yet:
//
//   ninja -C build/sanitize seastar_simulator_link_test
//   ./build/sanitize/seastar_simulator_link_test
//   ... ERROR seastar - FATAL: shared_ptr accessed on non-owner cpu
//
// `SEASTAR_DEBUG_SHARED_PTR`, which sanitize mode enables, checks a
// `shared_ptr`'s refcount against the `std::thread::id` which created it.
// The simulator gives each shard a thread of its own -- for its
// `thread_local` storage, not for parallelism -- so a `shared_ptr` moved
// between shards trips the check even though the shards never run at the same
// time and the access is safe.  Making sanitize mode usable here means either
// building it without that check or teaching the check about shards; until
// then, valgrind is what covers this scenario.
//

#include "engine.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/smp.hh>
#include <seastar/net/api.hh>
#include <seastar/rpc/rpc.hh>
#include <seastar/util/log.hh>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>

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

// The shards the server listens on.
//
// Both are in the same streaming domain and share the listening address, so
// a connection to it lands on one of them and the next lands on the other.
// Two are enough to put a stream's connection on a different shard from its
// parent, which is the point.
constexpr shard_id server_shards[] = {0, 1};

// Everything one server shard owns.
//
// Each listening shard has its own, reached from that shard only, so a plain
// pointer in a `thread_local` is enough; nothing else ever touches it.
struct server_state {
    test_protocol proto{serializer()};
    std::unique_ptr<test_protocol::server> server;
    // The stream work a verb handler started, which has to finish before the
    // run is over.  Only the shard which accepted the parent connection runs
    // a handler, so at most one of these is ever non-trivial.
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
// Every shard in `server_shards` runs this, and each registers itself in the
// engine's listener table for the same address.  That is what a sharded
// Seastar server does on a real host too: every shard listens on the address
// with `reuse_address`, and the connections arriving at it are spread over
// the shards which did.
future<> start_server() {
    test_logger.info("server: listening on port {}", server_port);
    g_server = new server_state();
    g_server->proto.set_logger(&rpc_logger);
    g_server->proto.register_handler(echo_verb,
            [] (int32_t token, rpc::source<sstring> source) {
        // This runs on the shard which accepted the *parent* connection.
        // The stream's own connection was accepted elsewhere and registered
        // here as a `foreign_ptr`, so `source` and the sink made from it
        // reach it across shards.
        test_logger.info("server: stream opened with token {}", token);
        check(token == 666, "server received the expected token");

        // The whole point of the scenario: the stream's connection was
        // accepted on a shard other than this one, and reaches this handler
        // as a `foreign_ptr`.  `source::get_id()` is the stream connection's
        // id, whose shard field is the shard which accepted it -- for a
        // stream connection that really is the owner shard, since the id is
        // stamped by `server::accept()` on the accepting shard.
        test_logger.info("server: handler is on shard {}, the stream's connection on shard {}",
                this_shard_id(), source.get_id().shard());
        check(source.get_id().shard() != this_shard_id(),
                "the stream's connection is owned by another shard");

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
    // connection belongs to when the stream's call arrives.  Here that
    // lookup is what crosses shards -- the domain is registered per shard,
    // and the stream's shard looks the parent's up on the parent's shard.
    rpc::server_options opts;
    opts.streaming_domain = rpc::streaming_domain_type(1);

    // The listening socket is made here rather than by `rpc::server`, because
    // it needs `reuse_address`: several shards listen on this one address.
    // The default load balancing algorithm spreads arriving connections over
    // them in turn, which is what puts the parent connection and the stream
    // connection on different shards.
    listen_options lo;
    lo.reuse_address = true;

    g_server->server = std::make_unique<test_protocol::server>(g_server->proto,
            opts, seastar::listen(socket_address(uint32_t(INADDR_LOOPBACK), server_port), lo));
    return make_ready_future<>();
}

// Shuts this shard's server down and waits for everything it started.
future<> stop_server() {
    test_logger.info("server: stopping");
    co_await std::move(g_server->stream_done);
    co_await g_server->server->shutdown();
    co_await g_server->server->stop();
    delete g_server;
    g_server = nullptr;
}

// The client half of the scenario, run on shard 2.
//
// The client is deliberately ordinary: one shard opens a connection, opens a
// stream on it, and uses it.  Nothing here is aware of shards at all.  The
// cross-shard machinery this scenario exercises is entirely on the server
// side, and is reached by the two connections the client makes landing on
// different server shards:
//
//   - the first connection is the RPC connection, accepted on one shard;
//
//   - the second is the stream's own connection.  It carries a STREAM_PARENT
//     feature naming the first connection, and the shard which accepts it
//     hands itself to the parent's shard with `smp::submit_to()`, where it is
//     registered as a `foreign_ptr` in the parent's `_streams` map.
//
// From then on the server's `sink` and `source` -- which live on the parent's
// shard, because that is where the verb handler runs -- reach the stream's
// connection through an `xshard_connection_ptr` and route every operation to
// `_con->get_owner_shard()`, which is the other shard.  So on the server:
//
//   - `sink_impl::_send_queue` is a `batched_queue` targeting the stream
//     connection's shard, so each echo enqueues a buffer which is batched and
//     handed to that shard to write;
//
//   - `send_buffer()`, running there, copies the buffer into a shard-local
//     one and hands the original back through `_delete_queue`, so it is freed
//     on the shard which allocated it;
//
//   - `source_impl::operator()` refills its buffers with `smp::submit_to()`
//     and copies each `rcv_buf` out of the foreign shard's memory;
//
//   - `sink_impl::close()` drains `_send_queue`, then hops to the stream's
//     shard to drain `_delete_queue` and write the end-of-stream frame.
//
// None of that is reached when a single shard owns both connections, which is
// why the server listens on two.
future<> run_client() {
    auto addr = socket_address(uint32_t(INADDR_LOOPBACK), server_port);

    test_logger.info("client: connecting to {}", addr);
    test_protocol proto(serializer{});
    proto.set_logger(&rpc_logger);
    test_protocol::client client(proto, addr);

    // Opening a stream takes a second connection to the same server, which is
    // why this needs a socket of its own -- and which is what lands on a
    // different server shard than the connection above.
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

    // Bring the server up on every shard which listens.  Each registers
    // itself for the same address, and the engine spreads the connections
    // arriving at it over them.
    co_await parallel_for_each(server_shards, [] (shard_id s) {
        return smp::submit_to(s, [] { return start_server(); });
    });

    // The client runs on shard 2, so getting it there and getting its result
    // back is itself a cross-shard call.
    test_logger.info("handing the client half to shard 2");
    co_await smp::submit_to(2, [] {
        return run_client();
    });

    co_await parallel_for_each(server_shards, [] (shard_id s) {
        return smp::submit_to(s, [] { return stop_server(); });
    });

    test_logger.info("scenario finished");
}

// The outcome of one run: what the engine did, or why the scenario failed.
struct run_result {
    simulator::engine::statistics stats;
    // Empty if the scenario succeeded.
    std::string failure;
};

// Runs the scenario once, under `policy`.
//
// Each run gets its own engine: an engine's state is the simulated program's
// state, so replaying the scenario under a different policy means starting
// from nothing.  A failure is returned rather than thrown, so that one bad
// interleaving does not hide the ones after it.
run_result run_once(simulator::scheduling_policy& policy) {
    run_result r;
    simulator::engine engine(4);
    engine.set_scheduling_policy(&policy);
    try {
        engine.run(scenario);
    } catch (const std::exception& e) {
        r.failure = e.what();
    } catch (...) {
        r.failure = "unknown exception";
    }
    r.stats = engine.stats();
    return r;
}

void report(const char* what, const run_result& r) {
    // Printed rather than logged: `run()` has returned, so there is no longer
    // an engine for the logger to ask which shard it is on.
    std::fprintf(stderr,
            "%s: %llu things: %llu tasks, %llu cross-shard calls, "
            "%llu network events, %llu timers%s%s\n",
            what,
            (unsigned long long) r.stats.total(),
            (unsigned long long) r.stats.tasks,
            (unsigned long long) r.stats.smp_messages,
            (unsigned long long) r.stats.network_messages,
            (unsigned long long) r.stats.timers,
            r.failure.empty() ? "" : " -- FAILED: ",
            r.failure.c_str());
}

} // anonymous namespace

int main() {
    // The baseline: the scenario under the engine's default order.
    simulator::default_scheduling_policy default_policy;
    auto baseline = run_once(default_policy);
    report("baseline", baseline);
    if (!baseline.failure.empty()) {
        std::fprintf(stderr, "the scenario fails under the default order; "
                "not sweeping\n");
        return 1;
    }

    // Then the sweep: freeze the i-th task, for every i, until i is past the
    // end of the run and there is no i-th task left to freeze.  Each
    // iteration is a different interleaving of the same program, and each is
    // expected to reach the same end.
    uint64_t failures = 0;
    uint64_t runs = 0;
    for (uint64_t i = 0; ; ++i) {
        simulator::freezing_scheduling_policy policy(i);
        auto r = run_once(policy);
        if (!policy.froze()) {
            // The run had no i-th task, so this run was the baseline and
            // every larger i would be too.
            break;
        }
        ++runs;
        char label[64];
        std::snprintf(label, sizeof(label), "freeze task %llu",
                (unsigned long long) i);
        if (!r.failure.empty()) {
            ++failures;
            report(label, r);
        }
    }

    std::fprintf(stderr, "sweep finished: %llu interleavings, %llu failed\n",
            (unsigned long long) runs, (unsigned long long) failures);
    return failures ? 1 : 0;
}
