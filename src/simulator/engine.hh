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
#include <cstdint>
#include <memory>
#include <optional>
#include <span>

namespace seastar {

class task;

namespace simulator {

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
/// The kinds of work the engine can run, in the order the default policy
/// prefers them.
///
/// This is the whole of the engine's choice: at every step it picks one of
/// these, and the engine then runs the oldest item of that kind.
enum class work_kind {
    task,             ///< a ready task of the program under simulation
    smp_message,      ///< a cross-shard call waiting to be delivered
    network_message,  ///< a network event waiting to be delivered
    timer,            ///< the earliest armed timer, which also moves time
};

/// What the engine has available to run at one moment.
///
/// Handed to the scheduling policy so it can choose without reaching into
/// the engine's queues.
struct available_work {
    /// How many ready tasks are waiting.
    size_t task_count = 0;
    /// The oldest waiting task, or null if there is none.  Identifies the
    /// task the default order would run next; not to be run or disposed of.
    task* next_task = nullptr;

    bool tasks = false;             ///< a ready task is waiting
    bool smp_messages = false;      ///< a cross-shard call is waiting
    bool network_messages = false;  ///< a network event is waiting
    bool timers = false;            ///< a timer is armed

    /// Whether there is anything at all to run.
    bool any() const noexcept {
        return tasks || smp_messages || network_messages || timers;
    }
};

/// Decides what the engine runs next.
///
/// The engine owns the queues and knows how to run what is in them; the
/// policy only says which kind to take from, which is the entirety of the
/// freedom a deterministic engine has.  Swapping the policy is therefore how
/// a different interleaving is explored without touching the engine.
///
/// A policy is asked once per step and must be deterministic itself if the
/// run is to be reproducible.
class scheduling_policy {
public:
    virtual ~scheduling_policy() = default;

    /// Chooses the kind of work to run next.
    ///
    /// \param avail what the engine has to offer; never empty of everything.
    /// \returns the kind to run, which must be one \c avail says is
    ///          available, or nothing to stop the run short of quiescence.
    virtual std::optional<work_kind> pick(const available_work& avail) = 0;

    /// Chooses which of the queued tasks to run, once \ref pick has asked
    /// for a task.
    ///
    /// \param queued the tasks waiting, oldest first; never empty.  The
    ///        pointers identify the tasks but must not be run or disposed of;
    ///        only the engine does that.
    /// \returns an index into \c queued.  The default is 0 -- the oldest
    ///          task -- which is what makes the run queue a queue.
    ///
    /// Choosing anything else runs a task ahead of one it was ordered behind,
    /// which is exactly what exploring a different interleaving means.  It is
    /// only sound because Seastar's run queue carries no ordering guarantee
    /// between independent tasks: a task which must follow another waits on
    /// its future instead, and a future which is not ready produces no queued
    /// task at all.  So a task in the queue is by construction one which is
    /// ready to run now.
    virtual size_t pick_task(std::span<task* const> queued) {
        (void) queued;
        return 0;
    }
};

/// The engine's default policy: the fixed priority described on \ref engine.
///
/// Tasks first, then cross-shard calls, then network events, and only when
/// nothing else can run does it let a timer move time forward.
class default_scheduling_policy final : public scheduling_policy {
public:
    virtual std::optional<work_kind> pick(const available_work& avail) override;
};

/// A policy which freezes one task and lets the rest of the run go past it.
///
/// It follows the default order until the \c i-th task is about to run.  That
/// task is then put aside: it stays at the head of the run queue but is never
/// chosen, so the engine runs every other task, cross-shard call, network
/// event and timer instead -- as far as the run can get without it.  Once
/// nothing else can run, the frozen task is released and the run finishes
/// under the default order.
///
/// This is the simplest family of interleavings which are not the default
/// one, and it is what makes the simulator able to find order-dependent bugs
/// rather than merely reproduce one schedule.  Sweeping \c i from 0 upwards
/// freezes each task in turn; once \c i is past the number of tasks the run
/// contains there is no \c i-th task to freeze, the run is the default one
/// again, and the sweep is done -- which \ref froze() reports.
class freezing_scheduling_policy final : public scheduling_policy {
    uint64_t _freeze_index;
    uint64_t _tasks_seen = 0;
    // Set while the frozen task is at the head of the queue and being skipped
    // over.  The index it sits at, which is 0 while nothing has been queued
    // in front of it -- the engine only ever appends, so it stays put.
    // The task put aside, identified by pointer: the engine's run queue is a
    // queue only for appends, and `schedule_urgent()` pushes to the front, so
    // the frozen task's position is not stable but its identity is.
    task* _frozen_task = nullptr;
    bool _froze = false;

public:
    /// Freezes the task which would be the \c freeze_index-th to run,
    /// counting from zero.
    explicit freezing_scheduling_policy(uint64_t freeze_index) noexcept
        : _freeze_index(freeze_index)
    { }

    virtual std::optional<work_kind> pick(const available_work& avail) override;
    virtual size_t pick_task(std::span<task* const> queued) override;

    /// Whether the run actually had a task to freeze.
    ///
    /// False once \c freeze_index has passed the number of tasks in the run,
    /// which is the signal that sweeping further changes nothing.
    bool froze() const noexcept { return _froze; }
};

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

    /// Installs the policy which decides what runs next.
    ///
    /// Must be called before \ref run(), and the policy must outlive it.
    /// Passing nothing restores \ref default_scheduling_policy.
    void set_scheduling_policy(scheduling_policy* policy) noexcept;

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

    /// How much work a run consisted of, broken down by kind.
    ///
    /// Counted by the engine's scheduling loop, so these are exactly the
    /// choices it made.  Because the engine is deterministic, a given program
    /// yields the same counts on every run; a change in them is a change in
    /// the program's behaviour, not in the weather.
    struct statistics {
        uint64_t tasks = 0;             ///< ready tasks run
        uint64_t smp_messages = 0;      ///< cross-shard calls delivered
        uint64_t network_messages = 0;  ///< network events delivered
        uint64_t timers = 0;            ///< timers fired

        /// The total number of things the engine ran.
        uint64_t total() const noexcept {
            return tasks + smp_messages + network_messages + timers;
        }
    };

    /// The work done so far, as counted during \ref run().
    statistics stats() const noexcept;
};

/// The engine the calling thread is running under.
///
/// Valid only inside \ref engine::run(); this is the global the free
/// functions of the Seastar API (`schedule()`, `listen()`, ...) reach the
/// engine through.
engine& current_engine() noexcept;

} // namespace simulator
} // namespace seastar
