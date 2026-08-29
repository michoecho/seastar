/*
 * Task-level tracing for Scylla, over the binary tracepoints in
 * modules/tracer of the cpp_template playground.
 *
 * The idea, and the event set, are a resurrection of the 2023 `old-tracer`
 * experiment: give every task an id that continuations inherit, so that the
 * chain of tasks belonging to one CQL request can be recovered from the trace
 * afterwards, and record enough switches and I/O boundaries to say where the
 * request's latency went -- on the CPU, in an I/O, or waiting to be scheduled.
 *
 * What is deliberately *not* here is any TRACEPOINT() call site. Every one of
 * them lives in src/core/scylla_tracer.cc, behind the out-of-line hooks
 * declared below, and there are two reasons for that:
 *
 *   - A tracepoint's static key is a block-scope static whose address the
 *     branch site needs as a link-time constant. In an inline or template
 *     function -- which is what a hook in a header would be -- that static has
 *     vague linkage and, in a shared library, is preemptible; the branch then
 *     fails to compile ("impossible constraint in 'asm'"). Out of line in one
 *     translation unit it is a plain local static.
 *
 *   - It keeps the whole tracepoint table inside libseastar.so, so the process
 *     has exactly one table, one static-key jump table and one registry --
 *     without the executable needing -Wl,--export-dynamic to share them.
 *
 * The cost is a function call per event rather than an inlined store. For a
 * prototype that is the right trade.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>


namespace seastar {

/// The next unused task id on this shard. A request handler takes one to open a
/// new chain; everything else inherits.
extern thread_local uint64_t fresh_task_id;

/// The task the shard is running right now. Zero means "none of ours".
extern thread_local uint64_t current_task_id;

/// A task id captured at the point of construction.
///
/// The default constructor is the whole mechanism: a `task_id` member that
/// nobody initialises explicitly records the task that was running when its
/// owner was created. That is how a continuation, an execution stage's work
/// item and an I/O descriptor all end up carrying the id of the request that
/// caused them, with no plumbing at any of the sites in between.
struct task_id {
    uint64_t _value;
    task_id(uint64_t id = current_task_id) noexcept : _value(id) {}
    operator uint64_t() const noexcept { return _value; }
};

/// Run a stretch of code as some other task, and put back the previous one.
///
/// [[nodiscard]] because the restore is the destructor's: a discarded temporary
/// would switch and switch back before the code it was meant to cover.
struct [[nodiscard]] switch_task {
    task_id _prev;
    explicit switch_task(uint64_t id) noexcept { current_task_id = id; }
    ~switch_task() { current_task_id = _prev; }
    switch_task(const switch_task&) = delete;
    switch_task& operator=(const switch_task&) = delete;
    uint64_t prev() const noexcept { return _prev; }
};

// The events. Each is one TRACEPOINT() in scylla_tracer.cc; see the file
// comment there for what the trace viewer makes of them.
void trace_run_task(uint64_t prev, uint64_t task) noexcept;
void trace_execution_stage(uint64_t prev, uint64_t task) noexcept;
void trace_cql_request(uint64_t prev, uint64_t task) noexcept;
void trace_semaphore_execute(uint64_t prev, uint64_t task) noexcept;
void trace_io_begin(uint64_t task, uint64_t io) noexcept;
void trace_io_end(uint64_t task, uint64_t io) noexcept;

/// A fresh I/O id, unique within this shard.
uint64_t next_io_id() noexcept;

/// This thread's rings, as a self-contained trace: the magic and a chunk per
/// level. Synchronous -- it copies -- so that a caller sweeping every shard
/// gets each one's rings as they were when it asked, and can write them out
/// afterwards at its leisure.
std::vector<std::byte> trace_snapshot();

/// The C++ source of a decoder for the tracepoints this process holds. Written
/// out beside a snapshot so the trace can be read without guessing which build
/// produced it.
std::string trace_decoder_source();

}
