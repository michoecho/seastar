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

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/util/noncopyable_function.hh>

#include <cstddef>
#include <memory>

namespace seastar::simulator {

/// The simulator's engine.
///
/// One engine owns everything the simulated program can observe: the shards,
/// the ready tasks, the timers, and the network.  A program runs under
/// simulation by constructing an engine and calling \ref engine::run().
///
/// The engine is deterministic.  Nothing in it depends on wall-clock time,
/// on the order in which the shard threads happen to be scheduled, or on the
/// host's network; every choice it makes -- which task runs next, when a
/// message is delivered, what time it is -- it makes itself, from state it
/// owns.
///
/// Shards are threads, but they are not concurrent: exactly one shard runs at
/// any moment, and only because the engine told it to.  The threads exist so
/// that each shard can have its own `thread_local` storage, which is what
/// Seastar code assumes of a shard; they are not a source of parallelism.
class engine {
public:
    /// The engine's state and its scheduling loop.
    ///
    /// Public because the implementations of the Seastar API in this library
    /// -- the free functions, the sockets, the timers -- are the engine's
    /// inside, not its users, and all of them need to reach it.
    class impl;

private:
    std::unique_ptr<impl> _impl;

public:
    /// Creates an engine with \c shard_count shards.
    ///
    /// The shard threads are started here, and are parked until the engine is
    /// given something to run on them.
    explicit engine(unsigned shard_count);
    ~engine();

    engine(const engine&) = delete;
    engine& operator=(const engine&) = delete;

    /// Runs \c func on shard 0, then runs the simulation to quiescence.
    ///
    /// The engine installs itself as the current engine, invokes \c func --
    /// which is where the program under simulation starts -- and then drives
    /// the simulation until there is nothing left to do: no ready task, no
    /// message in flight, and no armed timer.
    ///
    /// \throws whatever \c func's future resolves to, if it fails.
    void run(noncopyable_function<future<> ()> func);

    /// The number of shards in this engine.
    unsigned shard_count() const noexcept;
};

/// The engine the calling thread is running under.
///
/// Valid only inside \ref engine::run(); this is the global the free
/// functions of the Seastar API (`schedule()`, `listen()`, ...) reach the
/// engine through.
engine& current_engine() noexcept;

} // namespace seastar::simulator
