/*
 * Task-level tracing for Seastar, over the binary tracepoints in
 * modules/tracer of the cpp_template playground.
 *
 * The idea, and the event set, are a resurrection of the 2023 `old-tracer`
 * experiment: give every task an id that continuations inherit, so that the
 * chain of tasks belonging to one request can be recovered from the trace
 * afterwards, and record enough switches and I/O boundaries to say where the
 * request's latency went -- on the CPU, in an I/O, or waiting to be scheduled.
 *
 * What is here is what Seastar itself can see: the reactor's task switches,
 * the I/O queue, and RPC. An application built on Seastar has events of its
 * own -- Scylla's are in tracing/tracer.hh beside this repository -- and
 * declares them in a header of its own that includes this one. Nothing in
 * Seastar knows about them, and nothing here may.
 *
 * What is deliberately *not* here is nearly every TRACEPOINT() call site. They
 * live in src/core/tracer.cc, behind the out-of-line hooks declared below. The
 * one exception is run_task: it fires in the reactor's hottest loop, where its
 * static-ID record is emitted directly at the call site.
 *
 * Keeping the other events behind hooks is a convention, not a constraint, and
 * one thing keeps it: this header is included by task.hh and so by nearly every
 * translation unit that uses Seastar. A change to it is a full rebuild; a
 * change confined to the .cc is a few steps and a relink. Hooks out of line
 * mean that adding a tracepoint usually does not touch this file at all.
 *
 * It used to be a constraint, and a stronger one: every call site had to be
 * inside libseastar.so, so that the process had exactly one tracepoint table.
 * A jump entry named its key directly, and in PIC that address is only a
 * link-time constant for a symbol that cannot be preempted -- so a tracepoint
 * in an inline or template function in a shared library did not merely
 * misbehave, it failed to compile ("impossible constraint in 'asm'"). That is
 * no longer how it works. A jump entry now names static_keys::key_ref, a hidden
 * per-object slot pointing at the key, whose initialiser the loader resolves
 * with an ordinary data relocation; and JUMP_TABLE_ENTRY pushes its section
 * with the "?" flag, so the entry joins the COMDAT group of the inline function
 * it belongs to and is discarded with it. A gated tracepoint in a header works,
 * is exercised by modules/tracer/plugin/common_tracepoints.h, and the code
 * generator already tolerates the duplicate entries it produces. Each loaded
 * object registers a table of its own, so an executable linked against
 * libseastar.so may hold call sites too -- which is what lets an application's
 * events live in the application.
 *
 * The cost of the out-of-line hook is a function call per event rather than an
 * inlined store. For a prototype that is the right trade.
 */

#pragma once

#include "source_location/source_location.h"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>


namespace seastar {

/// The identity of one run of one process: a version-1 (time-based) UUID picked
/// once, shared by every shard, and carried by everything the run writes down.
///
/// A trace file says which *build* produced it -- that is the build ID -- but a
/// build ID is the same on every node of a cluster running the same package, and
/// the same again after a restart. What a reader of a distributed trace actually
/// has to tell apart is *processes*: which snapshot files came out of one
/// address space (so that their task ids, which are only unique within one, may
/// be merged), and which node is at the far end of an RPC connection. Neither
/// question has an answer in a build ID, an IP address (a node may be restarted
/// under the same one) or a shard number.
///
/// Time-based rather than random so that it also orders the runs it names: two
/// snapshot directories from one node sort by when the node booted.
struct boot_id {
    uint64_t msb = 0;  // time_low, time_mid, time_hi_and_version
    uint64_t lsb = 0;  // clock_seq and node

    friend bool operator==(const boot_id&, const boot_id&) noexcept = default;
    explicit operator bool() const noexcept { return msb != 0 || lsb != 0; }
};

/// This process's boot id, picked on the first call.
///
/// The reactor's constructor makes that first call, which is what "picked at
/// boot" means here: by the time any shard can trace or open a connection the
/// answer is fixed, and every shard gets the same one because the state behind
/// this is process-wide and not thread-local.
boot_id this_boot_id() noexcept;

/// A boot id in the canonical 8-4-4-4-12 hex form.
std::string boot_id_to_string(boot_id id);

/// The reverse, for a reader parsing one out of a snapshot's metadata. Returns a
/// zero id for anything that is not a UUID.
boot_id boot_id_from_string(std::string_view text);

/// Allocate a process-wide task id. A request handler takes one to open a new
/// chain; everything else inherits it, including work that moves to another
/// shard.
uint32_t next_task_id() noexcept;

/// The task the shard is running right now. Zero means "none of ours".
extern thread_local uint32_t current_task_id;

/// A task id captured at the point of construction.
///
/// The default constructor is the whole mechanism: a `task_id` member that
/// nobody initialises explicitly records the task that was running when its
/// owner was created. That is how a continuation, an execution stage's work
/// item and an I/O descriptor all end up carrying the id of the request that
/// caused them, with no plumbing at any of the sites in between.
struct task_id {
    uint32_t _value;
    task_id(uint32_t id = current_task_id) noexcept : _value(id) {}
    operator uint32_t() const noexcept { return _value; }
};

/// Run a stretch of code as some other task, and put back the previous one.
///
/// [[nodiscard]] because the restore is the destructor's: a discarded temporary
/// would switch and switch back before the code it was meant to cover.
struct [[nodiscard]] switch_task {
    task_id _prev;
    explicit switch_task(uint32_t id) noexcept { current_task_id = id; }
    ~switch_task() { current_task_id = _prev; }
    switch_task(const switch_task&) = delete;
    switch_task& operator=(const switch_task&) = delete;
};

// The events below are one TRACEPOINT() each in src/core/tracer.cc; see the
// file comment there for what the trace viewer makes of them. run_task is the
// exception: its TRACEPOINT_STATIC_ID() is in reactor.cc, where the hot loop
// picks the task up. `at` is where the task being run was created --
// task::location(), which is the then() call site or the co_await the coroutine
// suspended at. It is one address in one of the process's objects, so reading
// it back needs those objects; that is somebody else's job, and this process
// does not copy them anywhere. See "Source locations" in
// modules/trace-viewer/README.md.
// The reactor gave the cpu to one task queue, and took it back. A task queue is
// a scheduling group on this shard, so `scheduling_group` is that group's id --
// and every run_task between the two brackets ran under it. Only the begin
// carries it: the end is the same queue by construction, and a reader that has
// lost the begin (the ring evicted it) cannot use a group id it has nothing to
// attribute to anyway.
void trace_task_queue_run_begin(uint32_t scheduling_group) noexcept;
void trace_task_queue_run_end() noexcept;
void trace_execution_stage(uint32_t task) noexcept;

/// The I/O queue's events, which are the LTTng ones of
/// include/seastar/core/internal/io_trace_tp.h with a task id added: one
/// request is queued, dispatched to the backend, and then either completed or
/// cancelled, and `io` pairs the four up.
///
/// The request's shape -- what file, which direction, how much, and under which
/// priority class -- is on the *queued* record alone, because that is where the
/// I/O queue knows it and because none of it changes afterwards. The three that
/// follow carry the id and nothing else, so that the events a busy shard emits
/// most of stay small.
///
/// `task` is on the queued and completed records, and not on the two in
/// between: those are the boundaries of the wait a request's task actually
/// spends in the I/O, while dispatch and cancellation happen in whichever task
/// the queue's poller happened to be running.
void trace_io_queued(uint32_t task, uint64_t io, int32_t fd, uint32_t direction,
        uint32_t priority_class, uint64_t offset, uint64_t length) noexcept;
void trace_io_dispatched(uint64_t io) noexcept;
void trace_io_completed(uint32_t task, uint64_t io) noexcept;
void trace_io_cancelled(uint64_t io) noexcept;

/// RPC connection/message records are deliberately local to the process. The
/// connection id is not put on the wire; the trace viewer joins the two ends
/// using the endpoint metadata from the connection snapshot, and pairs the two
/// halves of one message by the per-direction sequence number.
///
/// What *is* on the wire is who the far end is: the RPC handshake carries a
/// PEER_IDENTITY feature both ways, so each end learns the other's boot id and
/// shard and puts them in its own connection records. That is not needed to
/// pair two ends whose snapshots are both open -- the endpoints already do that
/// -- but it is what lets a reader say which node a connection goes to when
/// only one side of it was captured, and it catches the case the endpoint join
/// cannot: an address reused by a node that has since restarted.
///
/// Because the identity is only known once the handshake is done,
/// `rpc_connection_open` is emitted *after* negotiation rather than when the
/// socket is set. A connection that never negotiates therefore has no records
/// at all, which is the right answer: it never carried a message either.
///
/// Attribution to a task is what makes those pairs useful, and neither end of
/// the wire is traced in the task that cares about the message: a message is
/// written by the connection's send loop and read by its receive loop, both of
/// which live for as long as the connection does.  So the two ends carry it
/// explicitly instead.  `rpc_message_sent`/`rpc_reply_sent` carry the task that
/// *enqueued* the buffer -- captured by `outgoing_entry`, which is constructed
/// in the caller -- and `rpc_request_handled` opens a fresh task chain for an
/// inbound request the way an application's request events do for a frame off
/// its own protocol, so that the work a replica does for one request is
/// separable from the connection's.
uint64_t next_rpc_connection_id() noexcept;
void trace_rpc_connection_open(uint64_t connection, std::string_view local,
        std::string_view remote, boot_id peer_boot, uint32_t peer_shard) noexcept;
void trace_rpc_connection_close(uint64_t connection, boot_id peer_boot,
        uint32_t peer_shard) noexcept;
void trace_rpc_message_sent(uint64_t connection, uint64_t sequence, uint32_t task) noexcept;
void trace_rpc_message_received(uint64_t connection, uint64_t sequence) noexcept;
void trace_rpc_reply_sent(uint64_t connection, uint64_t sequence, int64_t msg_id, uint32_t task) noexcept;
void trace_rpc_reply_received(uint64_t connection, uint64_t sequence, int64_t msg_id) noexcept;
void trace_rpc_connections_snapshot() noexcept;
void trace_rpc_request_handled(uint64_t connection, uint64_t sequence, uint32_t task) noexcept;

/// A fresh I/O id, unique within this shard.
uint64_t next_io_id() noexcept;

/// One record level of this thread's rings, as a self-contained trace, with
/// what a reader needs to know about it before decoding a byte.
///
/// One part per level rather than one blob for all of them, because the levels
/// are not the same kind of thing to whoever keeps the files: the debug ring is
/// an order of magnitude the larger and holds the per-task switches, while the
/// info ring holds the requests, the connection map and the stack samples. A
/// snapshot split by level can have its expensive half deleted and stay
/// readable. Each part carries the metadata chunk of its own accord, so it
/// decodes without the other.
///
/// The times are wall clock nanoseconds and they bracket the *records*, not the
/// snapshot: a ring evicts, so the oldest record in a busy shard's debug part
/// may be a second old while the info part beside it reaches back minutes.
struct trace_snapshot_part {
    unsigned level = 0;         // the tracer's event_level, as a number
    const char* level_name = "";  // ... and as "info" or "debug"
    uint64_t first_record_ns = 0;  // when the oldest surviving buffer went live
    uint64_t last_record_ns = 0;   // ... and when this snapshot stopped it
    std::vector<std::byte> data;
};

/// This thread's rings, a part per record level. Synchronous -- it copies -- so
/// that a caller sweeping every shard gets each one's rings as they were when
/// it asked, and can write them out afterwards at its leisure.
std::vector<trace_snapshot_part> trace_snapshot();

/// The build ID of the executable, as lowercase hex: the name under which a
/// reader of a snapshot asks for the objects its addresses point into.
std::string trace_build_id();

}
