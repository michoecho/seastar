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
// An alternative implementation of the non-core half of Seastar.
//
// `seastar-core` declares Seastar's core abstractions but does not define
// them; the definitions normally come from the `seastar` library, which
// implements them on top of the reactor.  This file is a second
// implementation -- a deterministic simulator -- meant to stand in for
// `seastar` when Seastar code is put under test.
//
// Everything the simulated program can observe belongs to one \ref engine:
// the shards it runs on, the tasks waiting to run, the timers, and the
// network.  The engine owns all of it, and drives it by itself; nothing here
// depends on the host's clock, on the host's network, or on when the OS
// happens to schedule a thread.  That is what makes a run reproducible.
//
// The three pieces are:
//
//   - the run queue.  A single FIFO of ready tasks for the whole engine, each
//     tagged with the shard which owns it.  The engine picks the task at the
//     front and runs it on its owner's thread.
//
//   - the shards.  A shard is a thread, parked on a condition variable until
//     the engine hands it a task and waits for it to finish.  Exactly one
//     shard runs at a time; the threads are there to give each shard its own
//     `thread_local` storage, which Seastar code assumes of a shard, not to
//     provide parallelism.  Since the shards never run concurrently, the
//     engine's own state needs no locking beyond the handoff itself.
//
//   - the network.  Entirely simulated: a `listen()` registers an address in
//     a table inside the engine, a `connect()` looks it up there, and the two
//     ends of the resulting connection are joined by in-memory pipes.  No
//     socket is ever created, and no byte leaves the process.
//
// The scheduling policy is the simplest one which is still faithful: messages
// are delivered as soon as they are sent, ready tasks run in FIFO order, and
// time only moves when there is nothing else to do -- so the simulation stays
// at one instant for as long as the program has work at that instant.
//

#include "engine.hh"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/iostream-impl.hh>
#include <seastar/core/internal/run_in_background.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/task.hh>
#include <seastar/core/timer.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/stack.hh>
#include <seastar/util/assert.hh>
#include <seastar/util/backtrace.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>
#include <seastar/util/log-impl.hh>

#include <arpa/inet.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <ostream>
#include <thread>
#include <utility>
#include <vector>

namespace seastar {

namespace {

// The body of the definitions which the simulator does not need yet.  A
// program which reaches one of them is asking for something the simulator has
// not been taught, and there is no sensible way to carry on, so say which one
// and stop.
[[noreturn]] [[gnu::noinline]]
void unimplemented(const char* what = __builtin_FUNCTION()) {
    std::fprintf(stderr, "seastar-simulator: unimplemented: %s\n", what);
    std::abort();
}

// The simulator's own logger, for the diagnostics the implementation itself
// emits -- abandoned futures and the like.
logger sim_logger("simulator");

// Renders an exception for a log message.
//
// {fmt} has no formatter for `std::exception_ptr` that the simulator can use
// without the reactor's, so unwrap it here.
sstring describe_exception(const std::exception_ptr& ex) noexcept {
    if (!ex) {
        return "<no exception>";
    }
    try {
        std::rethrow_exception(ex);
    } catch (const std::exception& e) {
        return e.what();
    } catch (...) {
        return "<unknown exception>";
    }
}

} // anonymous namespace

namespace simulator {

//
// The engine.
//

namespace {

// A task, plus what the engine needs to know about it to run it.
struct ready_task {
    task* t;
    shard_id shard;  // the shard which owns `t`, and the only one which may run it
};

// One end of a simulated connection: the bytes travelling in one direction.
//
// A pipe is the whole of the "network" between two sockets.  A write appends
// to `data` and wakes whoever is waiting to read; a read takes everything
// buffered, or parks a promise to be resolved by the next write.  Both ends
// live in the engine, so a "transmission" is a move of a buffer from one
// shard's view of the pipe to the other's.
//
// The pipe carries no notion of latency yet.  Delivery is immediate: a write
// resolves the reader's promise before it returns, which is what makes the
// default policy "messages are delivered immediately".
struct pipe {
    std::deque<temporary_buffer<char>> data;
    // Set while a reader is parked on an empty pipe.  There is at most one,
    // since a socket has a single input stream.
    std::optional<promise<temporary_buffer<char>>> waiter;
    // The shard the parked reader belongs to.  A promise may only be touched
    // on its own shard, so the engine hands the resolution back there.
    shard_id waiter_shard = 0;
    // Set once the writing end's close has arrived at the reading end.  A
    // reader which finds an empty, closed pipe gets EOF (an empty buffer),
    // which is how Seastar spells end of stream.
    //
    // The close arrives as a message rather than being applied where the
    // writer closes.  Closing a real connection is not something the peer
    // observes instantly -- the data already sent is still ahead of it -- and
    // a program which reads what was sent before the close has to see it.  If
    // the close were applied at the writer, a reader whose continuation had
    // not run yet would find the pipe shut and take that for a truncated
    // stream, even though the bytes it was about to read had arrived.
    bool closed = false;
    // Set as soon as the writing end closes, before the close arrives.  It
    // stops further writes without yet being visible to the reader.
    bool closing = false;
};

// A simulated connection: two pipes, one per direction, plus the addresses
// the two ends see.  Both sockets of a connection share one of these, so a
// write on one end is a read on the other with nothing in between.
struct connection {
    pipe client_to_server;
    pipe server_to_client;
    socket_address client_address;
    socket_address server_address;
};

// A server socket registered by `listen()`, on one shard.
//
// Several shards may listen on the same address -- that is what
// `reuse_address` is for, and how a sharded server is built -- so each gets a
// queue of its own here.  An arriving connection is steered to exactly one of
// them, and only that shard ever sees it.
struct listener_shard {
    // Connections which have arrived on this shard but have not been
    // accepted yet.
    std::deque<lw_shared_ptr<connection>> backlog;
    // Set while this shard's `accept()` is parked waiting for a connection.
    std::optional<promise<accept_result>> waiter;
    // Whether a `server_socket` for this shard still exists.  A shard whose
    // socket has been aborted is no longer a candidate for new connections,
    // but the entry stays until every shard is gone, so that the remaining
    // shards keep their queues.
    bool aborted = false;
};

// Everything listening on one address.
//
// Connecting clients only ever see what is in this table: `connect()` looks
// the destination address up here, and fails with ECONNREFUSED if nothing has
// been registered for it.  There is no host network involved at any point.
//
// This is the simulator's stand-in for a listening socket shared by every
// shard.  On a real host that sharing is done by the kernel (`SO_REUSEPORT`)
// or by the native stack, which steer each incoming connection to one shard;
// here the engine does the steering itself, by the same
// `load_balancing_algorithm` the caller asked for, so that which shard
// accepts a connection is a property of the connection rather than of who
// happened to call `accept()` first.
struct listener {
    socket_address address;
    // Indexed by shard id; empty for a shard which never called `listen()`.
    std::vector<std::optional<listener_shard>> shards;
    // How arriving connections are spread over the listening shards, and the
    // shard to use when that algorithm is `fixed`.  Taken from the first
    // `listen()` for this address; the later ones share it.
    server_socket::load_balancing_algorithm lba =
            server_socket::load_balancing_algorithm::default_;
    unsigned fixed_cpu = 0;
    // Connections steered so far, which `connection_distribution` counts to
    // decide where the next one goes.
    uint64_t accepted = 0;

    // The shards which could accept a connection right now.
    std::vector<shard_id> open_shards() const {
        std::vector<shard_id> ret;
        for (shard_id i = 0; i < shards.size(); ++i) {
            if (shards[i] && !shards[i]->aborted) {
                ret.push_back(i);
            }
        }
        return ret;
    }

    // Whether any shard is still listening.  The entry is dropped from the
    // engine's table once none is.
    bool any_open() const {
        return std::ranges::any_of(shards, [] (const std::optional<listener_shard>& s) {
            return s && !s->aborted;
        });
    }

    // Picks the shard which will accept a connection from `client_address`.
    //
    // This is the load balancing algorithm, applied to the shards which are
    // actually listening.  `connection_distribution` is meant to send a
    // connection to the least loaded shard; since a simulated connection
    // carries no load, round-robin over the listening shards is the same
    // thing and is deterministic, which matters more.
    shard_id pick_shard(const socket_address& client_address) {
        auto open = open_shards();
        SEASTAR_ASSERT(!open.empty());
        switch (lba) {
        case server_socket::load_balancing_algorithm::fixed:
            // The caller named a shard.  If it is not listening the
            // connection has nowhere to go, which is a bug in the caller
            // rather than something to paper over.
            SEASTAR_ASSERT(fixed_cpu < shards.size() && shards[fixed_cpu]
                    && !shards[fixed_cpu]->aborted
                    && "fixed_cpu shard is not listening");
            return fixed_cpu;
        case server_socket::load_balancing_algorithm::port:
            return open[client_address.port() % open.size()];
        case server_socket::load_balancing_algorithm::connection_distribution:
            return open[accepted++ % open.size()];
        }
        SEASTAR_ASSERT(false && "unknown load balancing algorithm");
    }
};

} // anonymous namespace

// The engine the current thread runs under.
//
// Set for the whole of `run()`, on every shard thread, which is what lets the
// free functions of the Seastar API find the engine without being passed one.
static thread_local engine* g_current_engine = nullptr;

engine& current_engine() noexcept {
    SEASTAR_ASSERT(g_current_engine && "not running under a simulator engine");
    return *g_current_engine;
}

// A timer, as the engine sees it.
//
// `timer<Clock>` is a core type whose members are private, so the engine
// cannot touch one directly.  Instead, arming a timer registers this record,
// whose `fire` and `is_queued` were produced inside `timer<Clock>`'s own
// member functions and therefore do have access.  That keeps the engine
// independent of which clock the timer belongs to.
struct engine_timer {
    // Nanoseconds since the start of the simulation, on the engine's clock.
    // Both supported clocks are steady and start at the same instant, so one
    // number orders every timer in the engine regardless of its clock.
    std::chrono::steady_clock::duration expiry;
    void* timer;              // the `timer<Clock>` this record stands for
    void (*fire)(void* timer); // runs the callback, and re-arms if periodic
    shard_id shard;           // the shard which armed it, and where it fires
    uint64_t seq;             // tie-breaker, so equal expiries fire in arm order
};

class engine::impl {
public:
    // A shard: a thread, and the slot through which the engine hands it work.
    struct shard {
        std::thread thread;
        std::mutex mutex;
        std::condition_variable cv;
        // The work to run on this shard.  Set by the engine, and taken by
        // the shard, which clears it -- so that finding it set is what tells
        // the shard there is something new to do.
        noncopyable_function<void ()>* work = nullptr;
        bool done = false;      // the shard finished the work handed to it
        bool stopping = false;  // the engine is shutting down; leave the loop
    };

    unsigned _shard_count;
    std::vector<std::unique_ptr<shard>> _shards;

    // Everything below is engine state.  It needs no locking: shards never
    // run concurrently, so whatever touches it -- the engine's own loop, or a
    // task running on a shard -- runs alone.

    // The ready tasks of every shard, in the order they became ready.  This
    // is the "global table of tasks which can be run next"; a task is picked
    // from the front and run on the shard which owns it.
    std::deque<ready_task> _run_queue;

    // Cross-shard calls in flight: functions which one shard asked another to
    // run and which have not started yet.  Kept apart from `_run_queue`
    // because they are messages rather than tasks -- they carry a function to
    // invoke, not a task to resume -- and because quiescence has to account
    // for them separately.
    struct smp_message {
        shard_id to;
        noncopyable_function<void ()> deliver;
    };
    std::deque<smp_message> _smp_messages;

    // Closes travelling from one end of a connection to the other.  Data is
    // delivered as it is written, so a pipe's buffer is all the "network"
    // there is for it; a close, which carries no data, needs a queue of its
    // own to sit in while the reader catches up with what was already sent.
    //
    // A close is carried by the same record as a cross-shard call, since it
    // is the same thing: something to run on a particular shard.
    std::deque<smp_message> _network_messages;

    // Armed timers, ordered by expiry.  Time is only allowed to move when
    // both queues above are empty, so a timer never runs while the program
    // still has work to do at the current instant.
    std::multimap<std::chrono::steady_clock::duration, engine_timer> _timers;
    uint64_t _timer_seq = 0;
    // The current simulated time, as a duration since the start of the run.
    std::chrono::steady_clock::duration _now{};

    // The simulated network.
    std::map<uint16_t, lw_shared_ptr<listener>> _listeners;
    // Handed out to connecting sockets which did not bind an address, so that
    // every connection has a distinct local address, as on a real host.
    uint16_t _next_ephemeral_port = 40000;

    // Set if the function passed to `run()` failed; rethrown from `run()`
    // once the simulation has quiesced.
    std::exception_ptr _run_exception;

    explicit impl(unsigned shard_count);
    ~impl();

    // Runs `f` on shard `s` and waits for it to return.
    //
    // This is the handoff: the engine parks on the condition variable while
    // the shard runs, and the shard parks on it again once it is done, so
    // only one of the two is ever running.
    void run_on(shard_id s, noncopyable_function<void ()> f);

    // Runs the simulation until quiescence.
    void run_to_quiescence();

    // Runs one step, if there is one.  Returns false if the engine has
    // quiesced.
    bool step();
};

namespace {

// The engine currently being run, as the free functions see it.
//
// `current_engine()` returns the public handle; this is the same engine's
// implementation, which is what everything in this file actually wants.
thread_local engine::impl* g_engine_impl = nullptr;

engine::impl& eng() noexcept {
    SEASTAR_ASSERT(g_engine_impl && "not running under a simulator engine");
    return *g_engine_impl;
}

} // anonymous namespace

engine::impl::impl(unsigned shard_count)
    : _shard_count(shard_count)
{
    SEASTAR_ASSERT(shard_count > 0);
    _shards.reserve(shard_count);
    for (unsigned i = 0; i != shard_count; ++i) {
        _shards.push_back(std::make_unique<shard>());
    }
}

engine::impl::~impl() {
    for (auto& s : _shards) {
        {
            std::lock_guard<std::mutex> lock(s->mutex);
            s->stopping = true;
        }
        s->cv.notify_one();
        if (s->thread.joinable()) {
            s->thread.join();
        }
    }
}

void engine::impl::run_on(shard_id s, noncopyable_function<void ()> f) {
    auto& sh = *_shards[s];
    {
        std::unique_lock<std::mutex> lock(sh.mutex);
        sh.work = &f;
        sh.done = false;
        sh.cv.notify_one();
        // The shard runs while we wait here, and we resume only once it is
        // parked again, so the two never run at the same time.
        sh.cv.wait(lock, [&] { return sh.done; });
    }
}

bool engine::impl::step() {
    // Ready tasks first: the program's own work at the current instant takes
    // precedence over everything else.
    if (!_run_queue.empty()) {
        auto rt = _run_queue.front();
        _run_queue.pop_front();
        run_on(rt.shard, [t = rt.t] {
            t->run_and_dispose();
        });
        return true;
    }

    // Then cross-shard messages.  Delivering one turns it into work on the
    // destination shard, which usually means new ready tasks.
    if (!_smp_messages.empty()) {
        auto msg = std::move(_smp_messages.front());
        _smp_messages.pop_front();
        run_on(msg.to, [&msg] {
            msg.deliver();
        });
        return true;
    }

    // Then the network's own messages.  A close only becomes visible to the
    // reader once every task ready at this instant has run, so a reader is
    // always given the chance to consume what was sent before it.
    if (!_network_messages.empty()) {
        auto msg = std::move(_network_messages.front());
        _network_messages.pop_front();
        run_on(msg.to, [&msg] {
            msg.deliver();
        });
        return true;
    }

    // Only when the program has nothing left to do at this instant does time
    // move, and then only as far as the earliest timer.  Everything else --
    // tasks, cross-shard calls, network closes -- is handled above, so
    // reaching here means the program is genuinely waiting on time.
    if (!_timers.empty()) {
        auto it = _timers.begin();
        auto et = it->second;
        _timers.erase(it);
        _now = std::max(_now, et.expiry);
        run_on(et.shard, [&et] {
            et.fire(et.timer);
        });
        return true;
    }

    return false;
}

void engine::impl::run_to_quiescence() {
    while (step()) {
    }
}

engine::engine(unsigned shard_count)
    : _impl(std::make_unique<impl>(shard_count))
{ }

engine::~engine() = default;

unsigned engine::shard_count() const noexcept {
    return _impl->_shard_count;
}

void engine::run(noncopyable_function<future<> ()> func) {
    auto& e = *_impl;

    // Start the shard threads.  Each one installs the engine and its own
    // shard id in its `thread_local`s -- which is what makes it a shard --
    // and then parks, waiting to be handed work.
    for (unsigned i = 0; i != e._shard_count; ++i) {
        auto& sh = *e._shards[i];
        sh.thread = std::thread([this, &e, &sh, i] {
            g_current_engine = this;
            g_engine_impl = &e;
            *internal::this_shard_id_ptr() = i;
            for (;;) {
                noncopyable_function<void ()>* work = nullptr;
                {
                    std::unique_lock<std::mutex> lock(sh.mutex);
                    sh.cv.wait(lock, [&] { return sh.work != nullptr || sh.stopping; });
                    if (sh.stopping) {
                        return;
                    }
                    // Take the work, so that the next thing found in the slot
                    // is the next thing the engine puts there.
                    work = std::exchange(sh.work, nullptr);
                }
                (*work)();
                {
                    std::lock_guard<std::mutex> lock(sh.mutex);
                    sh.done = true;
                }
                sh.cv.notify_one();
            }
        });
    }

    // The engine's own thread needs the globals too: the free functions may
    // be reached from here, and `smp::shard_count()` is read outside a shard.
    g_current_engine = this;
    g_engine_impl = &e;
    smp::set_shard_count(e._shard_count);

    // The program under simulation starts on shard 0.
    e.run_on(0, [&] {
        internal::run_in_background(futurize_invoke(func).handle_exception(
                [&e] (std::exception_ptr ex) {
            e._run_exception = std::move(ex);
        }));
    });

    e.run_to_quiescence();

    g_current_engine = nullptr;
    g_engine_impl = nullptr;

    if (e._run_exception) {
        std::rethrow_exception(e._run_exception);
    }
}

} // namespace simulator

//
// Tasks and the scheduler.
//

void schedule(task* t) noexcept {
    auto& e = simulator::eng();
    e._run_queue.push_back({t, this_shard_id()});
}

void schedule_checked(task* t) noexcept {
    schedule(t);
}

void schedule_urgent(task* t) noexcept {
    // "Urgent" only asks for the task to run before the ones already queued.
    auto& e = simulator::eng();
    e._run_queue.push_front({t, this_shard_id()});
}

future<> yield() noexcept {
    // Hand the continuation to the back of the run queue, so that everything
    // ready at this instant gets a turn before the caller resumes.
    struct yielder final : task {
        promise<> _pr;
        virtual void run_and_dispose() noexcept override {
            _pr.set_value();
            delete this;
        }
        virtual task* waiting_task() noexcept override { return _pr.waiting_task(); }
    };
    auto* y = new yielder;
    auto f = y->_pr.get_future();
    schedule(y);
    return f;
}

//
// Per-shard state.
//
// These accessors are out-of-line only in a shared-library build; in a static
// build the headers define them inline.
//

namespace internal {

#ifdef SEASTAR_BUILD_SHARED_LIBS

shard_id* this_shard_id_ptr() noexcept {
    static thread_local shard_id g_this_shard_id;
    return &g_this_shard_id;
}

const preemption_monitor*& get_need_preempt_var() {
    static preemption_monitor bootstrap_preemption_monitor;
    static thread_local const preemption_monitor* g_need_preempt = &bootstrap_preemption_monitor;
    return g_need_preempt;
}

scheduling_group* current_scheduling_group_ptr() noexcept {
    static thread_local scheduling_group sg;
    return &sg;
}

#endif

[[noreturn]]
void assert_fail(const char* msg, const char* file, int line, const char* func) {
    std::fprintf(stderr, "%s:%d: %s: assertion `%s' failed\n", file, line, func, msg);
    std::abort();
}

void run_in_background(future<> f) {
    // Nothing is waiting for `f`, but the simulation is not finished until it
    // resolves, and a failure must not be lost.  Keeping a continuation alive
    // on it does both: it holds the future until it resolves, and the engine
    // does not quiesce while a continuation is outstanding.
    (void)std::move(f).handle_exception([] (std::exception_ptr ex) {
        sim_logger.error("Background future failed: {}", describe_exception(ex));
    });
}

[[noreturn]] void throw_bad_alloc() { throw std::bad_alloc(); }
[[noreturn]] void throw_sstring_overflow() { throw std::overflow_error("sstring overflow"); }
[[noreturn]] void throw_sstring_out_of_range() { throw std::out_of_range("sstring out of range"); }

}

//
// Futures and promises.
//
// The future/promise machinery is almost entirely in the header; what an
// implementation owes it is the handful of out-of-line pieces below.  None of
// them are about scheduling, so they say the same thing here as they do in
// the reactor -- apart from `do_wait()`, which cannot be supported without
// Seastar threads, and `report_failed_future()`, which reports through the
// simulator's logger.
//

future_state_base::future_state_base(current_exception_future_marker) noexcept
    : future_state_base(std::current_exception()) { }

static std::exception_ptr make_nested(std::exception_ptr&& inner, future_state_base&& old) noexcept {
    std::exception_ptr outer = std::move(old).get_exception();
    nested_exception nested{std::move(inner), std::move(outer)};
    return std::make_exception_ptr<nested_exception>(std::move(nested));
}

future_state_base::future_state_base(nested_exception_marker, future_state_base&& n, future_state_base&& old) noexcept {
    std::exception_ptr inner = std::move(n).get_exception();
    if (!old.failed()) {
        new (this) future_state_base(std::move(inner));
    } else {
        new (this) future_state_base(make_nested(std::move(inner), std::move(old)));
    }
}

future_state_base::future_state_base(nested_exception_marker, future_state_base&& old) noexcept {
    if (!old.failed()) {
        new (this) future_state_base(current_exception_future_marker());
    } else {
        new (this) future_state_base(make_nested(std::current_exception(), std::move(old)));
    }
}

void future_state_base::ignore() noexcept {
    switch (_u.st) {
    case state::invalid:
    case state::future:
    case state::result_unavailable:
        SEASTAR_ASSERT(0 && "invalid state for ignore");
    case state::result:
        _u.st = state::result_unavailable;
        break;
    default:
        // Ignore the exception
        _u.take_exception();
    }
}

void future_state_base::rethrow_exception() && {
    // Move ex out so future::~future() knows we've handled it
    std::rethrow_exception(std::move(*this).get_exception());
}

void future_state_base::rethrow_exception() const& {
    std::rethrow_exception(_u.ex);
}

// The header declares this `extern template`, so the implementation owes the
// instantiation.  The template body is in the header and needs nothing from
// here beyond `future_state_base`'s constructor above.
template
future<void> current_exception_as_future() noexcept;

broken_promise::broken_promise() : logic_error("broken promise") { }

nested_exception::nested_exception(std::exception_ptr inner, std::exception_ptr outer) noexcept
    : inner(std::move(inner)), outer(std::move(outer)) {}

nested_exception::nested_exception(nested_exception&&) noexcept = default;

nested_exception::nested_exception(const nested_exception&) noexcept = default;

const char* nested_exception::what() const noexcept {
    return "seastar::nested_exception";
}

[[noreturn]] void nested_exception::rethrow_nested() const {
    std::rethrow_exception(outer);
}

namespace internal {

void promise_base::move_it(promise_base&& x) noexcept {
    // Don't use std::exchange to make sure x's values are nulled even
    // if &x == this.
    _task = x._task;
    x._task = nullptr;
#ifdef SEASTAR_DEBUG_PROMISE
    _task_shard = x._task_shard;
#endif
    _state = x._state;
    x._state = nullptr;
    _future = x._future;
    if (auto* fut = _future) {
        fut->detach_promise();
        fut->_promise = this;
    }
}

static void set_to_broken_promise(future_state_base& state) noexcept {
    try {
        // Constructing broken_promise may throw (std::logic_error ctor is not noexcept).
        state.set_exception(std::make_exception_ptr(broken_promise{}));
    } catch (...) {
        state.set_exception(std::current_exception());
    }
}

promise_base::promise_base(promise_base&& x) noexcept {
    move_it(std::move(x));
}

void promise_base::clear_on_broken() noexcept {
    SEASTAR_ASSERT(_state && !_state->available());
    set_to_broken_promise(*_state);
    if (_task) {
        ::seastar::schedule(std::exchange(_task, nullptr));
    }
    if (_future) {
        _future->detach_promise();
    }
}

promise_base& promise_base::operator=(promise_base&& x) noexcept {
    clear();
    move_it(std::move(x));
    return *this;
}

void promise_base::set_to_current_exception() noexcept {
    set_exception(std::current_exception());
}

#ifdef SEASTAR_DEBUG_PROMISE

void promise_base::assert_task_shard() const noexcept {
    if (_task_shard >= 0 && static_cast<shard_id>(_task_shard) != this_shard_id()) {
        std::fprintf(stderr, "seastar-simulator: promise task was set on shard %d "
                "but made ready on shard %u\n", _task_shard, this_shard_id());
        std::abort();
    }
}

#endif

void future_base::do_wait() noexcept {
    // `future::get()` on an unresolved future blocks the calling Seastar
    // thread until it resolves.  The simulator has no Seastar threads -- a
    // shard thread is running a task, and blocking it would deadlock the
    // engine, since nothing else can run while it is blocked -- so there is
    // nothing to switch to.  Code under simulation has to use continuations
    // or coroutines.
    unimplemented("future::get() on an unresolved future (seastar::thread is "
            "not supported under simulation)");
}

void future_base::set_coroutine(task& coroutine) noexcept {
    SEASTAR_ASSERT(_promise);
    _promise->set_task(&coroutine);
}

void report_failed_future(const std::exception_ptr& eptr) noexcept {
    sim_logger.warn("Exceptional future ignored: {}", describe_exception(eptr));
}

void report_failed_future(const future_state_base& state) noexcept {
    report_failed_future(state._u.ex);
}

void report_failed_future(future_state_base::any&& state) noexcept {
    report_failed_future(std::move(state).take_exception());
}

}

//
// Loops.
//
// `parallel_for_each_state` is pure future plumbing -- it waits on the
// futures it was given and resolves one promise when they are all done -- so
// it says the same thing here as it does in the reactor.
//

parallel_for_each_state::parallel_for_each_state(size_t n) {
    _incomplete.reserve(n);
}

future<> parallel_for_each_state::get_future() {
    auto ret = _result.get_future();
    wait_for_one();
    return ret;
}

void parallel_for_each_state::add_future(future<>&& f) {
    _incomplete.push_back(std::move(f));
}

void parallel_for_each_state::wait_for_one() noexcept {
    // Process from back to front, on the assumption that the front futures
    // are likely to complete earlier than the back futures.

    // Skip over futures that happen to be complete already.
    while (!_incomplete.empty() && _incomplete.back().available()) {
        if (_incomplete.back().failed()) {
            _ex = _incomplete.back().get_exception();
        }
        _incomplete.pop_back();
    }

    // If there's an incomplete future, wait for it.
    if (!_incomplete.empty()) {
        internal::set_callback(std::move(_incomplete.back()), static_cast<continuation_base<>*>(this));
        // This future's state will be collected in run_and_dispose(), so we can drop it.
        _incomplete.pop_back();
        return;
    }

    // Everything completed, report a result.
    if (__builtin_expect(bool(_ex), false)) {
        _result.set_exception(std::move(_ex));
    } else {
        _result.set_value();
    }
    delete this;
}

void parallel_for_each_state::run_and_dispose() noexcept {
    if (_state.failed()) {
        _ex = std::move(_state).get_exception();
    }
    _state = {};
    wait_for_one();
}

//
// Semaphores.
//

const char* broken_semaphore::what() const noexcept { return "semaphore broken"; }
const char* semaphore_timed_out::what() const noexcept { return "semaphore timed out"; }
const char* semaphore_aborted::what() const noexcept { return "semaphore aborted"; }

semaphore_timed_out semaphore_default_exception_factory::timeout() noexcept { return semaphore_timed_out(); }
broken_semaphore semaphore_default_exception_factory::broken() noexcept { return broken_semaphore(); }
semaphore_aborted semaphore_default_exception_factory::aborted() noexcept { return semaphore_aborted(); }

//
// Clocks and timers.
//

// Out-of-line only in a shared-library build; otherwise the header defines it
// as an inline variable.
#ifdef SEASTAR_BUILD_SHARED_LIBS
thread_local lowres_clock::time_point lowres_clock::_now;
#endif

namespace simulator {

// The current simulated time, as a duration since the start of the run.
//
// Both clocks the simulator supports are steady and both start here, so one
// number serves for both: a `steady_clock::time_point` and a
// `lowres_clock::time_point` for the same instant differ only in type.
std::chrono::steady_clock::duration now() noexcept {
    return eng()._now;
}

} // namespace simulator

namespace {

// Converts a time point of any of the simulator's clocks into the engine's
// single notion of time.
template <typename Clock>
std::chrono::steady_clock::duration to_engine_time(typename Clock::time_point tp) noexcept {
    return std::chrono::duration_cast<std::chrono::steady_clock::duration>(
            tp.time_since_epoch());
}

} // anonymous namespace

// The out-of-line members of `timer<Clock>` are defined by the
// implementation, not by the header, since arming a timer means handing it to
// whatever is driving time.  As in the reactor, they are defined once as a
// template and then instantiated for each clock that is used.
//
// The engine cannot reach into a timer -- its members are private -- so what
// it gets instead is a function pointer produced here, inside a member, where
// the private members are in scope.  `arm()` registers that together with the
// expiry, and the engine calls it when the timer's turn comes.
//
// What is here is the plumbing, not a working clock.  `lowres_clock::_now` is
// never advanced, and `steady_clock_type` is `std::chrono::steady_clock`,
// whose `now()` belongs to the host and cannot be made to follow simulated
// time -- so a deadline taken from it lands in the timer queue as a host
// timestamp rather than an engine one.  Making time under simulation mean
// what a caller expects needs both clocks brought under the engine, which is
// its own change; nothing exercises timers yet, so it is left until something
// does.

template <typename Clock>
timer<Clock>::~timer() {
    if (_queued) {
        auto& e = simulator::eng();
        for (auto it = e._timers.begin(); it != e._timers.end(); ++it) {
            if (it->second.timer == this) {
                e._timers.erase(it);
                break;
            }
        }
    }
}

template <typename Clock>
void timer<Clock>::arm(time_point until, std::optional<duration> period) noexcept {
    arm_state(until, period);
    auto& e = simulator::eng();
    auto fire = [] (void* p) noexcept {
        auto* t = static_cast<timer*>(p);
        t->_queued = false;
        t->_expired = true;
        t->_armed = false;
        if (t->_period) {
            // Re-arm before running the callback, as the reactor does, so
            // that a callback which cancels the timer wins.
            t->readd_periodic();
        }
        t->_callback();
    };
    auto expiry = to_engine_time<Clock>(until);
    e._timers.emplace(expiry, simulator::engine_timer{
        .expiry = expiry,
        .timer = this,
        .fire = fire,
        .shard = this_shard_id(),
        .seq = e._timer_seq++,
    });
}

template <typename Clock>
void timer<Clock>::readd_periodic() noexcept {
    // The period is measured from now, so a periodic timer never accumulates
    // drift from however long its callback took.
    arm(Clock::now() + _period.value(), _period);
}

template <typename Clock>
bool timer<Clock>::cancel() noexcept {
    if (!_armed) {
        return false;
    }
    _armed = false;
    if (_queued) {
        auto& e = simulator::eng();
        for (auto it = e._timers.begin(); it != e._timers.end(); ++it) {
            if (it->second.timer == this) {
                e._timers.erase(it);
                break;
            }
        }
        _queued = false;
    }
    return true;
}

template class timer<steady_clock_type>;
template class timer<lowres_clock>;

//
// Networking.
//
// Every socket below is backed by a `simulator::connection` living in the
// engine.  Writing appends to one of the connection's two pipes and resolves
// whatever is parked on the other end; reading takes from the other pipe, or
// parks.  There is no host socket anywhere, and no data ever leaves the
// process.
//

namespace net {

namespace {

using simulator::connection;
using simulator::pipe;

// Hands a buffer to whoever is reading the far end of `p`, or buffers it.
//
// Resolving the reader's promise is the delivery of the message, and it
// happens here, synchronously with the write -- that is the "messages are
// delivered immediately" half of the default scheduling policy.  The reader's
// promise belongs to the reader's shard, so if the two ends are on different
// shards, the resolution is sent there rather than done here.
void deliver(pipe& p, temporary_buffer<char> buf) {
    if (p.waiter) {
        auto pr = std::move(*p.waiter);
        p.waiter.reset();
        if (p.waiter_shard == this_shard_id()) {
            pr.set_value(std::move(buf));
        } else {
            auto& e = simulator::eng();
            e._smp_messages.push_back({p.waiter_shard,
                    [pr = std::move(pr), buf = std::move(buf)] () mutable {
                pr.set_value(std::move(buf));
            }});
        }
        return;
    }
    p.data.push_back(std::move(buf));
}

// Closes the writing end of `p`, so that the reader eventually sees EOF.
//
// The close is queued rather than applied here.  Everything already written
// is ahead of it in the reader's view of the connection, and the reader has
// to be able to consume that before the stream ends -- which under this
// engine means every task ready at this instant gets to run first.  Applying
// the close where the writer stands would instead let a reader whose
// continuation had not run yet find the pipe already shut, and read a stream
// that was truncated by nothing but scheduling order.
void close_pipe(pipe& p) {
    if (p.closing) {
        return;  // already closed, or a close is already on its way
    }
    p.closing = true;
    auto& e = simulator::eng();
    // The close is delivered on whichever shard the reader is parked on, if
    // one is, since waking it means touching its promise.
    e._network_messages.push_back({p.waiter ? p.waiter_shard : this_shard_id(), [&p] {
        p.closed = true;
        if (p.waiter) {
            // Wake the parked reader with EOF, which is how Seastar spells
            // end of stream.
            auto pr = std::move(*p.waiter);
            p.waiter.reset();
            pr.set_value(temporary_buffer<char>());
        }
    }});
}

// The reading end of one direction of a connection.
class simulated_data_source final : public data_source_impl {
    lw_shared_ptr<connection> _conn;
    // Which of the connection's two pipes this end reads from.
    pipe connection::* _in;

public:
    simulated_data_source(lw_shared_ptr<connection> conn, pipe connection::* in) noexcept
        : _conn(std::move(conn)), _in(in) { }

    virtual future<temporary_buffer<char>> get() override {
        auto& p = (*_conn).*_in;
        if (!p.data.empty()) {
            auto buf = std::move(p.data.front());
            p.data.pop_front();
            return make_ready_future<temporary_buffer<char>>(std::move(buf));
        }
        if (p.closed) {
            // End of stream.
            return make_ready_future<temporary_buffer<char>>();
        }
        SEASTAR_ASSERT(!p.waiter && "concurrent reads from one socket");
        p.waiter.emplace();
        p.waiter_shard = this_shard_id();
        return p.waiter->get_future();
    }

    virtual future<> close() override {
        // Closing the reading end does not close the connection; the peer
        // keeps whatever it has already sent.  Nothing more will be read
        // from this pipe, so there is nothing to do.
        return make_ready_future<>();
    }
};

// The writing end of one direction of a connection.
class simulated_data_sink final : public data_sink_impl {
    lw_shared_ptr<connection> _conn;
    // Which of the connection's two pipes this end writes into.
    pipe connection::* _out;

public:
    simulated_data_sink(lw_shared_ptr<connection> conn, pipe connection::* out) noexcept
        : _conn(std::move(conn)), _out(out) { }

    virtual future<> put(std::span<temporary_buffer<char>> data) override {
        auto& p = (*_conn).*_out;
        if (p.closing) {
            return make_exception_future<>(std::system_error(EPIPE, std::system_category(),
                    "write to a closed connection"));
        }
        for (auto& buf : data) {
            if (buf.empty()) {
                continue;
            }
            // The caller may reuse the storage behind the span as soon as we
            // return, so take ownership of every buffer now.
            deliver(p, std::move(buf));
        }
        return make_ready_future<>();
    }

    virtual future<> flush() override {
        // Writes are delivered as they are made, so there is never anything
        // buffered here to flush.
        return make_ready_future<>();
    }

    virtual future<> close() override {
        close_pipe((*_conn).*_out);
        return make_ready_future<>();
    }

    virtual size_t buffer_size() const noexcept override {
        return 128 * 1024;
    }
};

// A connected socket: one end of a `simulator::connection`.
//
// Which end it is decides which pipe it reads and which it writes; the two
// ends of a connection are otherwise identical.
class simulated_connected_socket final : public connected_socket_impl {
    lw_shared_ptr<connection> _conn;
    bool _is_server_end;
    // Resolved when the peer shuts down its writing end.
    std::optional<promise<>> _input_shutdown;

    pipe connection::* in_pipe() const noexcept {
        return _is_server_end ? &connection::client_to_server : &connection::server_to_client;
    }
    pipe connection::* out_pipe() const noexcept {
        return _is_server_end ? &connection::server_to_client : &connection::client_to_server;
    }

public:
    simulated_connected_socket(lw_shared_ptr<connection> conn, bool is_server_end) noexcept
        : _conn(std::move(conn)), _is_server_end(is_server_end) { }

    virtual data_source source() override {
        return data_source(std::make_unique<simulated_data_source>(_conn, in_pipe()));
    }

    virtual data_sink sink() override {
        return data_sink(std::make_unique<simulated_data_sink>(_conn, out_pipe()));
    }

    virtual void shutdown_input() override {
        close_pipe((*_conn).*in_pipe());
    }

    virtual void shutdown_output() override {
        close_pipe((*_conn).*out_pipe());
    }

    // The simulated network has no congestion control and no keepalives, so
    // the knobs which tune them are accepted and ignored.
    virtual void set_nodelay(bool) override { }
    virtual bool get_nodelay() const override { return true; }
    virtual void set_keepalive(bool) override { }
    virtual bool get_keepalive() const override { return false; }
    virtual void set_keepalive_parameters(const keepalive_params&) override { }
    virtual keepalive_params get_keepalive_parameters() const override {
        return tcp_keepalive_params{};
    }
    virtual void set_sockopt(int, int, const void*, size_t) override { }
    virtual int get_sockopt(int, int, void*, size_t) const override { return 0; }

    virtual socket_address local_address() const noexcept override {
        return _is_server_end ? _conn->server_address : _conn->client_address;
    }

    virtual socket_address remote_address() const noexcept override {
        return _is_server_end ? _conn->client_address : _conn->server_address;
    }

    virtual future<> wait_input_shutdown() override {
        auto& p = (*_conn).*in_pipe();
        if (p.closing) {
            return make_ready_future<>();
        }
        // Nothing under simulation waits for this yet, so rather than track
        // another waiter on the pipe, say so if it is ever reached.
        unimplemented("connected_socket::wait_input_shutdown()");
    }
};

// An unconnected socket, as returned by `make_socket()`.
//
// It keeps a handle on the connection it establishes, because `shutdown()`
// has to terminate it: that is how a client stops a connection it opened,
// and the connection's read loop only ends when its socket is shut down.
class simulated_socket final : public socket_impl {
    bool _shutdown = false;
    lw_shared_ptr<connection> _conn;

public:
    virtual future<connected_socket> connect(socket_address sa, socket_address local, transport) override {
        if (_shutdown) {
            return make_exception_future<connected_socket>(
                    std::system_error(ECONNABORTED, std::system_category(), "connect aborted"));
        }
        auto& e = simulator::eng();
        // A connecting client only ever sees what `listen()` registered.
        // There is no host network to fall back on: an address nobody is
        // listening on is refused, exactly as it would be.
        auto it = e._listeners.find(sa.port());
        if (it == e._listeners.end() || !it->second->any_open()) {
            return make_exception_future<connected_socket>(
                    std::system_error(ECONNREFUSED, std::system_category(), "connection refused"));
        }
        auto& l = *it->second;

        if (local.is_unspecified()) {
            local = socket_address(uint32_t(INADDR_LOOPBACK), e._next_ephemeral_port++);
        }

        auto conn = make_lw_shared<connection>();
        conn->client_address = local;
        conn->server_address = sa;
        _conn = conn;

        // The client's end is ready as soon as the connection exists; the
        // server's end is handed to whoever accepts it.
        auto client_end = connected_socket(
                std::make_unique<simulated_connected_socket>(conn, false));

        // Which shard gets this connection is decided here, by the listener's
        // load balancing algorithm, and not by which shard happens to be in
        // `accept()`.  That is what makes the destination a property of the
        // connection: the shard is chosen even if nobody is waiting yet, and
        // the connection sits in that shard's backlog until it accepts.
        auto deliver_to = l.pick_shard(local);
        auto& ls = *l.shards[deliver_to];

        if (ls.waiter) {
            // That shard is parked in accept(); hand it the connection.
            auto pr = std::move(*ls.waiter);
            ls.waiter.reset();
            auto make_result = [conn, client_address = local] {
                return accept_result{
                    connected_socket(std::make_unique<simulated_connected_socket>(conn, true)),
                    client_address,
                };
            };
            if (deliver_to == this_shard_id()) {
                pr.set_value(make_result());
            } else {
                e._smp_messages.push_back({deliver_to,
                        [pr = std::move(pr), make_result = std::move(make_result)] () mutable {
                    pr.set_value(make_result());
                }});
            }
        } else {
            ls.backlog.push_back(std::move(conn));
        }

        return make_ready_future<connected_socket>(std::move(client_end));
    }

    virtual void set_reuseaddr(bool) override { }
    virtual bool get_reuseaddr() const override { return false; }

    virtual void shutdown() override {
        _shutdown = true;
        if (_conn) {
            // Terminate the connection in both directions, as shutting a
            // real socket down would.
            close_pipe(_conn->client_to_server);
            close_pipe(_conn->server_to_client);
        }
    }
};

// A listening socket: a handle on one shard's slot in an entry of the
// engine's listener table.
//
// The entry is shared with every other shard listening on the same address,
// but the queue this socket accepts from is its own, so a connection steered
// to another shard is invisible here.
class simulated_server_socket final : public server_socket_impl {
    lw_shared_ptr<simulator::listener> _listener;
    // The shard which called `listen()`, and therefore the slot in
    // `_listener->shards` this socket owns.  A `server_socket` may not be
    // used from another shard, so this never changes.
    shard_id _shard;

public:
    simulated_server_socket(lw_shared_ptr<simulator::listener> l, shard_id shard) noexcept
        : _listener(std::move(l)), _shard(shard) { }

    virtual future<accept_result> accept() override {
        SEASTAR_ASSERT(this_shard_id() == _shard
                && "server_socket accepted on a shard other than the one which listened");
        auto& ls = *_listener->shards[_shard];
        if (ls.aborted) {
            return make_exception_future<accept_result>(
                    std::system_error(ECONNABORTED, std::system_category(), "accept aborted"));
        }
        if (!ls.backlog.empty()) {
            auto conn = std::move(ls.backlog.front());
            ls.backlog.pop_front();
            return make_ready_future<accept_result>(accept_result{
                connected_socket(std::make_unique<simulated_connected_socket>(conn, true)),
                conn->client_address,
            });
        }
        SEASTAR_ASSERT(!ls.waiter && "concurrent accept() on one server socket");
        ls.waiter.emplace();
        return ls.waiter->get_future();
    }

    virtual void abort_accept() override {
        auto& l = *_listener;
        auto& ls = *l.shards[_shard];
        ls.aborted = true;
        // A parked accept() has to be woken, or the engine would never
        // quiesce: nothing else is going to resolve it.
        if (ls.waiter) {
            auto pr = std::move(*ls.waiter);
            ls.waiter.reset();
            pr.set_exception(std::system_error(ECONNABORTED, std::system_category(),
                    "accept aborted"));
        }
        // The address stays reachable for as long as any shard is still
        // listening on it; it is only unregistered once the last one is gone.
        if (!l.any_open()) {
            simulator::eng()._listeners.erase(l.address.port());
        }
    }

    virtual socket_address local_address() const override {
        return _listener->address;
    }
};

} // anonymous namespace

data_source connected_socket_impl::source(connected_socket_input_stream_config) {
    // Under simulation there is nothing for the buffer-size hints to tune.
    return source();
}

} // namespace net

// The socket handles are thin: they own a `*_impl` and forward to it.  These
// are the same one-liners the reactor's implementation has, and are here for
// the same reason -- `net/api.hh` declares them but leaves them to whichever
// implementation is linked.

connected_socket::connected_socket() noexcept { }

connected_socket::connected_socket(std::unique_ptr<net::connected_socket_impl> csi) noexcept
    : _csi(std::move(csi)) { }

connected_socket::connected_socket(connected_socket&&) noexcept = default;
connected_socket& connected_socket::operator=(connected_socket&&) noexcept = default;
connected_socket::~connected_socket() { }

input_stream<char> connected_socket::input(connected_socket_input_stream_config csisc) {
    return input_stream<char>(_csi->source(csisc));
}

output_stream<char> connected_socket::output(size_t buffer_size) {
    output_stream_options opts;
    // Batching would let a flush complete before the data reaches the peer,
    // which would put a message in flight that the engine does not know
    // about.  Writing straight through keeps every byte accounted for.
    opts.batch_flushes = false;
    return output_stream<char>(_csi->sink(), buffer_size, opts);
}

void connected_socket::set_nodelay(bool nodelay) { _csi->set_nodelay(nodelay); }
bool connected_socket::get_nodelay() const { return _csi->get_nodelay(); }
void connected_socket::set_keepalive(bool keepalive) { _csi->set_keepalive(keepalive); }
bool connected_socket::get_keepalive() const { return _csi->get_keepalive(); }
void connected_socket::set_keepalive_parameters(const net::keepalive_params& p) {
    _csi->set_keepalive_parameters(p);
}
net::keepalive_params connected_socket::get_keepalive_parameters() const {
    return _csi->get_keepalive_parameters();
}
void connected_socket::set_sockopt(int level, int optname, const void* data, size_t len) {
    _csi->set_sockopt(level, optname, data, len);
}
int connected_socket::get_sockopt(int level, int optname, void* data, size_t len) const {
    return _csi->get_sockopt(level, optname, data, len);
}
socket_address connected_socket::local_address() const noexcept { return _csi->local_address(); }
socket_address connected_socket::remote_address() const noexcept { return _csi->remote_address(); }
void connected_socket::shutdown_output() { _csi->shutdown_output(); }
void connected_socket::shutdown_input() { _csi->shutdown_input(); }
future<> connected_socket::wait_input_shutdown() { return _csi->wait_input_shutdown(); }

socket::socket(std::unique_ptr<net::socket_impl> si) noexcept : _si(std::move(si)) { }
socket::socket(socket&&) noexcept = default;
socket& socket::operator=(socket&&) noexcept = default;
socket::~socket() { }

future<connected_socket> socket::connect(socket_address sa, socket_address local, transport proto) {
    return _si->connect(sa, local, proto);
}
void socket::set_reuseaddr(bool reuseaddr) { _si->set_reuseaddr(reuseaddr); }
bool socket::get_reuseaddr() const { return _si->get_reuseaddr(); }
void socket::shutdown() { _si->shutdown(); }

server_socket::server_socket() noexcept { }

server_socket::server_socket(std::unique_ptr<net::server_socket_impl> ssi) noexcept
    : _ssi(std::move(ssi)) { }

server_socket::server_socket(server_socket&& ss) noexcept
    : _ssi(std::move(ss._ssi)), _aborted(ss._aborted) { }

server_socket& server_socket::operator=(server_socket&& ss) noexcept {
    if (this != &ss) {
        _ssi = std::move(ss._ssi);
        _aborted = ss._aborted;
    }
    return *this;
}

server_socket::~server_socket() { }

future<accept_result> server_socket::accept() {
    if (_aborted) {
        return make_exception_future<accept_result>(
                std::system_error(ECONNABORTED, std::system_category(), "accept aborted"));
    }
    return _ssi->accept();
}

void server_socket::abort_accept() {
    _aborted = true;
    _ssi->abort_accept();
}

socket_address server_socket::local_address() const noexcept {
    return _ssi->local_address();
}

server_socket listen(socket_address sa, listen_options opts) {
    auto& e = simulator::eng();
    auto [it, inserted] = e._listeners.emplace(sa.port(), lw_shared_ptr<simulator::listener>());
    if (!inserted && !opts.reuse_address) {
        throw std::system_error(EADDRINUSE, std::system_category(),
                "address already in use");
    }
    if (inserted) {
        it->second = make_lw_shared<simulator::listener>();
        it->second->address = sa;
        it->second->shards.resize(e._shard_count);
        // The first `listen()` for an address fixes how connections to it are
        // spread; the shards which join later share that decision, as they
        // share the socket it stands for.
        it->second->lba = opts.lba;
        it->second->fixed_cpu = opts.fixed_cpu;
    }
    auto& l = *it->second;
    auto shard = this_shard_id();
    // One `listen()` per shard per address.  A second one would be a second
    // socket accepting from the same queue, which is not something the real
    // API offers either.
    if (l.shards[shard] && !l.shards[shard]->aborted) {
        throw std::system_error(EADDRINUSE, std::system_category(),
                "address already in use on this shard");
    }
    l.shards[shard].emplace();
    return server_socket(std::make_unique<net::simulated_server_socket>(it->second, shard));
}

server_socket listen(socket_address sa) {
    return listen(sa, listen_options());
}

socket make_socket() {
    return socket(std::make_unique<net::simulated_socket>());
}

//
// Addresses.
//
// The simulator's network is identified by port alone -- a listener table
// keyed on the port is the whole of it -- but the addresses still have to be
// real `socket_address`es, since that is what the API passes around.  These
// are the few members RPC and the engine actually use.
//

socket_address::socket_address() noexcept
    : addr_length(sizeof(::sockaddr_in))
{
    std::memset(&u, 0, sizeof(u));
    u.in.sin_family = AF_INET;
}

socket_address::socket_address(uint32_t ipv4, uint16_t p) noexcept
    : addr_length(sizeof(::sockaddr_in))
{
    std::memset(&u, 0, sizeof(u));
    u.in.sin_family = AF_INET;
    u.in.sin_port = htons(p);
    u.in.sin_addr.s_addr = htonl(ipv4);
}

socket_address::socket_address(uint16_t p) noexcept
    : socket_address(uint32_t(INADDR_ANY), p)
{ }

bool socket_address::operator==(const socket_address& a) const noexcept {
    if (u.sa.sa_family != a.u.sa.sa_family) {
        return false;
    }
    if (u.sa.sa_family != AF_INET) {
        unimplemented("socket_address::operator== for non-AF_INET addresses");
    }
    return u.in.sin_port == a.u.in.sin_port
            && u.in.sin_addr.s_addr == a.u.in.sin_addr.s_addr;
}

::in_port_t socket_address::port() const noexcept {
    return ntohs(u.in.sin_port);
}

bool socket_address::is_wildcard() const noexcept {
    return u.sa.sa_family == AF_INET && u.in.sin_addr.s_addr == htonl(INADDR_ANY)
            && u.in.sin_port == 0;
}

bool socket_address::is_unspecified() const noexcept {
    return is_wildcard();
}

std::ostream& operator<<(std::ostream& os, const socket_address& a) {
    if (a.u.sa.sa_family != AF_INET) {
        return os << "{unknown address family}";
    }
    char buf[INET_ADDRSTRLEN];
    ::inet_ntop(AF_INET, &a.u.in.sin_addr, buf, sizeof(buf));
    return os << buf << ':' << a.port();
}

namespace internal {

void add_to_flush_poller(output_stream<char>&) noexcept {
    // Only reached with `output_stream_options::batch_flushes`, which the
    // simulator's sockets do not turn on.
    unimplemented();
}

}

//
// Cross-shard messaging.
//

unsigned smp::_shard_count = 1;
smp* smp::_this_smp = nullptr;

void smp::set_shard_count(unsigned n) noexcept {
    _shard_count = n;
    static smp the_smp;
    _this_smp = &the_smp;
}

namespace internal {

future<> smp_submit_to_erased(shard_id t, smp_submit_to_options, noncopyable_function<future<> ()> func) {
    SEASTAR_ASSERT(t != this_shard_id());
    auto& e = simulator::eng();
    SEASTAR_ASSERT(t < e._shard_count);

    // The promise stays on this shard -- a promise may only be touched on the
    // shard which owns it -- so what crosses is the function to run and, when
    // it is done, a message back saying so.
    auto pr = make_lw_shared<promise<>>();
    auto f = pr->get_future();
    auto from = this_shard_id();

    e._smp_messages.push_back({t, [func = std::move(func), pr, from] () mutable {
        // Now on the destination shard.
        (void)futurize_invoke(func).then_wrapped([pr, from] (future<> res) mutable {
            auto ex = res.failed() ? res.get_exception() : std::exception_ptr();
            auto& e = simulator::eng();
            e._smp_messages.push_back({from, [pr, ex = std::move(ex)] () mutable {
                // Back on the submitting shard, where the promise lives.
                if (ex) {
                    pr->set_exception(std::move(ex));
                } else {
                    pr->set_value();
                }
            }});
        });
    }});

    return f;
}

} // namespace internal

//
// Memory.
//

namespace memory::internal {
#ifdef __cpp_constinit
thread_local constinit volatile int critical_alloc_section = 0;
#else
__thread volatile int critical_alloc_section = 0;
#endif
}

//
// Logging.
//
// Logging is implemented rather than stubbed: a simulator whose diagnostics
// go nowhere is much harder to work with than one whose do not.  Messages are
// written to stderr, tagged with the simulated time and the shard, so that a
// log reads as a trace of the run.
//

#ifdef SEASTAR_BUILD_SHARED_LIBS
thread_local bool logger::silent = false;
#endif

void logger::do_log(log_level level, log_writer& writer) {
    static const char* level_names[] = {"ERROR", "WARN", "INFO", "DEBUG", "TRACE"};

    char stack_buf[512];
    internal::log_buf buf(stack_buf, sizeof(stack_buf));
    auto it = buf.back_insert_begin();
    it = writer(it);

    auto now = std::chrono::duration_cast<std::chrono::microseconds>(
            simulator::now()).count();
    auto view = buf.view();
    std::fprintf(stderr, "%lld.%06lld [shard %2u] %-5s %s - %.*s\n",
            static_cast<long long>(now / 1000000), static_cast<long long>(now % 1000000),
            this_shard_id(),
            level_names[static_cast<int>(level)],
            name().c_str(),
            static_cast<int>(view.size()), view.data());
}

void logger::failed_to_log(std::exception_ptr ex, fmt::string_view fmt, std::source_location loc) noexcept {
    try {
        std::fprintf(stderr, "%s:%u: failed to log message \"%.*s\"\n",
                loc.file_name(), loc.line(),
                static_cast<int>(fmt.size()), fmt.data());
    } catch (...) {
        // Nothing useful left to do.
    }
    (void)ex;
}

void log_exception_trace(log_level) noexcept {
    // Backtraces of exception construction are a reactor debugging aid; the
    // simulator has no equivalent yet.
}

// The logger's own out-of-line members.  The reactor's implementation keeps
// a registry of loggers so that log levels can be set by name at runtime;
// under simulation there is nothing to configure them from, so a logger is
// just its name and its level.

logger::logger(sstring name) : _name(std::move(name)) { }

logger::logger(logger&& x) : _name(std::move(x._name)), _level(x._level.load(std::memory_order_relaxed)) { }

logger::~logger() { }

namespace internal {

void log_buf::free_buffer() noexcept {
    if (_own_buf) {
        std::free(_begin);
    }
}

log_buf::log_buf()
    : _begin(static_cast<char*>(std::malloc(512)))
    , _end(_begin + 512)
    , _current(_begin)
    , _own_buf(true)
{ }

log_buf::log_buf(char* external_buf, size_t size) noexcept
    : _begin(external_buf)
    , _end(_begin + size)
    , _current(_begin)
    , _own_buf(false)
{ }

log_buf::~log_buf() {
    free_buffer();
}

void log_buf::realloc_buffer_and_append(char c) noexcept {
    if (_alloc_failure) {
        // Already failed to grow once; drop the rest of the message rather
        // than keep trying.
        return;
    }
    const auto old_size = size();
    const auto new_size = old_size * 2;
    auto* new_buf = static_cast<char*>(std::malloc(new_size));
    if (!new_buf) {
        _alloc_failure = true;
        return;
    }
    std::memcpy(new_buf, _begin, old_size);
    free_buffer();
    _begin = new_buf;
    _end = new_buf + new_size;
    _current = new_buf + old_size;
    _own_buf = true;
    *_current++ = c;
}

}

//
// Backtraces.
//
// Backtraces are a reactor debugging aid: they say where in the host's stack
// a task was created.  Under simulation the interesting history of a task is
// the sequence of engine steps which led to it, not the host stack, which is
// only ever the engine's own loop.  So these are empty, and exist because
// `backtraced<>` -- which RPC uses to decorate its exceptions -- refers to
// them.
//

bool operator==(const frame& a, const frame& b) noexcept {
    return a.so == b.so && a.addr == b.addr;
}

tasktrace::~tasktrace() { }

bool tasktrace::operator==(const tasktrace& o) const noexcept {
    return _hash == o._hash && _main == o._main && _prev == o._prev;
}

saved_backtrace current_backtrace() noexcept {
    return saved_backtrace();
}

tasktrace current_tasktrace() noexcept {
    return tasktrace();
}

simple_backtrace current_backtrace_tasklocal() noexcept {
    return simple_backtrace();
}

} // namespace seastar

// A backtrace under simulation is always empty, so it prints as nothing.
auto fmt::formatter<seastar::tasktrace>::format(const seastar::tasktrace&,
        fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "<no backtrace under simulation>");
}

// {fmt}'s rendering of an exception, as the core headers declare it.  This is
// the simulator's version of what `util/log.cc` provides for the reactor:
// the exception's `what()`, which is all the simulator's diagnostics need.
auto fmt::formatter<seastar::internal::formattable_exception_ptr>::format(
        const seastar::internal::formattable_exception_ptr& fe,
        fmt::format_context& ctx) const -> decltype(ctx.out()) {
    if (!fe.eptr) {
        return fmt::format_to(ctx.out(), "<no exception>");
    }
    try {
        std::rethrow_exception(fe.eptr);
    } catch (const std::exception& e) {
        return fmt::format_to(ctx.out(), "{}", e.what());
    } catch (...) {
        return fmt::format_to(ctx.out(), "<unknown exception>");
    }
}

namespace seastar {

//
// Metrics.
//
// Metrics are accepted and discarded.  Nothing under simulation reads them
// back, and the registration path only has to not get in the way.
//

namespace metrics {

metric_groups::metric_groups() noexcept { }
metric_groups::~metric_groups() { }

metric_groups& metric_groups::add_group(const group_name_type&,
        const std::initializer_list<metric_definition>&) {
    return *this;
}

metric_definition::metric_definition(const impl::metric_definition_impl&) noexcept { }
metric_definition::~metric_definition() { }

namespace impl {

metric_definition_impl::metric_definition_impl(metric_name_type name, metric_type type,
        metric_function f, description d, std::vector<label_instance>,
        std::vector<label>)
    : name(std::move(name)), type(type), f(std::move(f)), d(std::move(d))
{
    // The labels are dropped along with everything else; nothing reads them.
}

metric_definition_impl& metric_definition_impl::set_skip_when_empty(bool skip) noexcept {
    _skip_when_empty = skip_when_empty(skip);
    return *this;
}

}

}

}
