/*
 * The one translation unit holding tracepoint call sites.
 *
 * See include/seastar/core/scylla_tracer.hh for why they are all here rather
 * than inlined at the sites that call them.
 *
 * The event set is the 2023 `old-tracer` experiment's, renamed from numbers to
 * tracepoints with named parameters:
 *
 *   run_task{prev, task, at}     the reactor picked a task off a run queue, and
                                where that task was created (task::location())
 *   execution_stage{prev, task}  an execution stage ran a queued work item
 *   semaphore_execute{prev, task} the reader concurrency semaphore's loop ran
 *                                a queued read, as the task that asked for it
 *   cql_request{prev, task}      a CQL frame arrived and opened a new chain
 *   io_begin{task, io}           a task submitted an I/O and is now waiting
 *   io_end{task, io}             that I/O completed
 *
 * The first three are *switches*: `task` is the id the shard is running from
 * here on, `prev` the one it was running. A trace viewer recovers one request
 * by taking every record whose task is the id a cql_request opened, which works
 * because a task id is inherited by every continuation, work item and I/O
 * descriptor created while it is current -- see task_id in the header.
 */

#include <seastar/core/scylla_tracer.hh>
#include <seastar/core/scylla_tracer_control.hh>

#include <seastar/core/rendezvous.hh>
#include <seastar/core/shard_id.hh>

#include "tracer/codegen.h"
#include "tracer/tracer.h"

namespace seastar {

// Seeded with the shard in the top bits, because a task id is *not* shard-local
// once it has been minted: a request coordinated on one shard reaches a tablet
// on another, and the continuations that run there inherit the id. A trace
// viewer merging the shards' files therefore sees one request's records in two
// files under one id, and that only works if no other shard can mint the same
// number. A dynamic initialiser, so it is evaluated per thread on first use --
// by which time the reactor has set this thread's shard id.
thread_local uint64_t fresh_task_id = (uint64_t(this_shard_id()) << 48) | 1;
thread_local uint64_t current_task_id = 0;

namespace {

// Smaller than the tracer's defaults because on a reactor thread these come out
// of the shard's memory budget: a --memory=2G two-shard run has 1G a shard.
// Still room for something like half a million events before the ring starts
// evicting.
constexpr size_t info_capacity = 4 * 1024 * 1024;
constexpr size_t debug_capacity = 32 * 1024 * 1024;
constexpr size_t metadata_capacity = 64 * 1024;

// Deliberately leaked, and that is the point rather than an oversight. A
// thread_local with a destructor would be torn down at thread exit while the
// reactor is still running tasks on the way out, and every hook below writes
// through `local_tracer` with no null check. Leaking one ring per thread costs
// a fixed amount of memory and removes the whole shutdown ordering question.
[[gnu::noinline]] void install_tracer() {
    static thread_local tracer::trace_buffers* buffers =
            new tracer::trace_buffers(info_capacity, debug_capacity, metadata_capacity);
    tracer::local_tracer = buffers;
}

// Constructing the rings walks the loaded objects and writes the metadata
// prologue, so it cannot happen at static-init time; the first event on a
// thread pays for it.
inline void ensure_tracer() {
    if (tracer::local_tracer == nullptr) [[unlikely]] {
        install_tracer();
    }
}

}

void trace_run_task(uint64_t prev, uint64_t task, srcloc::location at) noexcept {
    ensure_tracer();
    TRACEPOINT(tracer::event_level::debug, "run_task", "prev", prev, "task", task, "at", at);
}

void trace_execution_stage(uint64_t prev, uint64_t task) noexcept {
    ensure_tracer();
    TRACEPOINT(tracer::event_level::debug, "execution_stage", "prev", prev, "task", task);
}

void trace_cql_request(uint64_t prev, uint64_t task) noexcept {
    ensure_tracer();
    TRACEPOINT(tracer::event_level::info, "cql_request", "prev", prev, "task", task);
}

void trace_semaphore_execute(uint64_t prev, uint64_t task) noexcept {
    ensure_tracer();
    TRACEPOINT(tracer::event_level::debug, "semaphore_execute", "prev", prev, "task", task);
}

void trace_io_begin(uint64_t task, uint64_t io) noexcept {
    ensure_tracer();
    TRACEPOINT(tracer::event_level::debug, "io_begin", "task", task, "io", io);
}

void trace_io_end(uint64_t task, uint64_t io) noexcept {
    ensure_tracer();
    TRACEPOINT(tracer::event_level::debug, "io_end", "task", task, "io", io);
}

future<bool> set_tracepoints_enabled(bool enabled) {
    // The action runs on shard 0 with every other shard parked in its poll
    // loop, which is the only place this may happen: it rewrites the branch
    // instruction at every tracepoint call site, and a shard executing one of
    // them as it changes is undefined. It touches no seastar state and does
    // not allocate -- it walks the tracepoint table and calls mprotect and
    // memcpy -- which is what a rendezvous action has to be.
    return run_at_rendezvous([enabled] { tracer::set_all_tracepoints_enabled(enabled); });
}

uint64_t next_io_id() noexcept {
    // Shard in the top bits and never zero, for the same reason as
    // fresh_task_id above: a viewer merging the shards' files pairs an io_begin
    // with its io_end by this number alone.
    static thread_local uint64_t next = (uint64_t(this_shard_id()) << 48) | 1;
    return next++;
}

std::vector<std::byte> trace_snapshot() {
    ensure_tracer();
    return tracer::collect_trace(*tracer::local_tracer);
}

std::string trace_decoder_source() {
    return tracer::generate_decoder_source();
}

}
