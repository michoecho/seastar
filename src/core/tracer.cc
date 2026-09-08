/*
 * The translation unit holding nearly all of Seastar's tracepoint call sites.
 *
 * See include/seastar/core/tracer.hh for why nearly all of them are here
 * rather than inlined at the sites that call them. The run_task tracepoint is
 * deliberately in reactor.cc: it is the hottest event and uses a static id.
 *
 * The event set is the 2023 `old-tracer` experiment's, renamed from numbers to
 * tracepoints with named parameters:
 *
 *   run_task{task, at}           the reactor picked a task off a run queue, and
                                where that task was created (task::location());
                                emitted directly in reactor.cc with static id 1
 *   task_queue_run_{begin,end}   the reactor gave the cpu to one task queue and
 *                                took it back; the begin carries the scheduling
 *                                group id every task_run between the two ran under
 *   execution_stage{task}        an execution stage ran a queued work item
 *   io_queued{task, io, ...}     a task submitted an I/O and is now waiting, and
 *                                what the request is
 *   io_dispatched{io}            the I/O queue handed it to the backend
 *   io_completed{task, io}       it completed
 *   io_cancelled{io}             it was cancelled before dispatch
 *   rpc_connection_{open,close}  lifecycle records with endpoint metadata
 *   rpc_message_{sent,received}  direction-local transport sequence records
 *   rpc_reply_{sent,received}    the same sequence plus the RPC request id
 *   rpc_connections_snapshot_*   the live connection map, emitted at dump time
 *   rpc_request_handled          an inbound request opened a task chain
 *
 * One event is *not* here: stacktrace_sample, which lives in
 * src/core/stacktrace_sampler.cc beside the perf ring it is drained from. The
 * rule the file comment above states is that a call site should be an
 * out-of-line function, not that they must all be in one file, and that one has
 * nothing to say to the hooks below.
 *
 * Events belonging to what is *built on* Seastar are not here either, and may
 * not be: they live in the application, in a translation unit of its own that
 * uses the same TRACEPOINT() macros against its own tracepoint table. Scylla's
 * are in tracing/tracer.cc beside this repository.
 *
 * The first several are *switches*: `task` is the id the shard is running from
 * here on. A trace viewer recovers one request by taking every record whose
 * task is the id its opening event allocated, which works because a task id is
 * inherited by every continuation, work item and I/O descriptor created while
 * it is current -- see task_id in the header.
 */

#include <seastar/core/tracer.hh>
#include <seastar/core/tracer_control.hh>

#include <seastar/core/rendezvous.hh>
#include <seastar/core/stacktrace_sampler.hh>
#include <seastar/core/shard_id.hh>

#include "tracer/tracer.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <random>
#include <string>
#include <unordered_map>

namespace seastar {

// --- the boot id --------------------------------------------------------------

namespace {

// A version-1 UUID: the wall clock in 100ns units since 1582-10-15, a random
// clock sequence, and a random node with the multicast bit set -- which is what
// RFC 4122 says to put there when the MAC address is not to be used, and there
// is no reason to leak one here.
boot_id make_boot_id() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    const auto unix_100ns =
            std::chrono::duration_cast<std::chrono::duration<uint64_t, std::ratio<1, 10'000'000>>>(now)
                    .count();
    // 1582-10-15 to 1970-01-01, in 100ns units.
    constexpr uint64_t uuid_epoch_offset = 0x01b2'1dd2'1381'4000ULL;
    const uint64_t timestamp = unix_100ns + uuid_epoch_offset;

    std::random_device entropy;
    const auto random64 = [&entropy] {
        return (uint64_t(entropy()) << 32) | entropy();
    };

    const uint64_t time_low = timestamp & 0xffff'ffffULL;
    const uint64_t time_mid = (timestamp >> 32) & 0xffffULL;
    const uint64_t time_hi = (timestamp >> 48) & 0x0fffULL;

    boot_id id;
    id.msb = (time_low << 32) | (time_mid << 16) | (1ULL << 12) | time_hi;
    const uint64_t clock_seq = (random64() & 0x3fffULL) | 0x8000ULL;  // variant 10
    const uint64_t node = (random64() & 0xffff'ffff'ffffULL) | (1ULL << 40);  // multicast
    id.lsb = (clock_seq << 48) | node;
    return id;
}

}  // namespace

boot_id this_boot_id() noexcept {
    // Process-wide, not thread_local: every shard has to answer the same, and a
    // magic static is what makes concurrent reactor constructions agree on one
    // without a lock of our own.
    static const boot_id id = make_boot_id();
    return id;
}

std::string boot_id_to_string(boot_id id) {
    char text[37];
    std::snprintf(text, sizeof(text), "%08x-%04x-%04x-%04x-%012llx",
            unsigned((id.msb >> 32) & 0xffff'ffffULL),
            unsigned((id.msb >> 16) & 0xffffULL),
            unsigned(id.msb & 0xffffULL),
            unsigned((id.lsb >> 48) & 0xffffULL),
            (unsigned long long)(id.lsb & 0xffff'ffff'ffffULL));
    return std::string(text);
}

boot_id boot_id_from_string(std::string_view text) {
    // Hex digits in order, dashes ignored: 32 of them or it is not a UUID.
    uint64_t halves[2] = {0, 0};
    unsigned digits = 0;
    for (const char c : text) {
        if (c == '-') {
            continue;
        }
        unsigned value;
        if (c >= '0' && c <= '9') {
            value = unsigned(c - '0');
        } else if (c >= 'a' && c <= 'f') {
            value = unsigned(c - 'a') + 10;
        } else if (c >= 'A' && c <= 'F') {
            value = unsigned(c - 'A') + 10;
        } else {
            return {};
        }
        if (digits >= 32) {
            return {};
        }
        halves[digits / 16] = (halves[digits / 16] << 4) | value;
        ++digits;
    }
    if (digits != 32) {
        return {};
    }
    return boot_id{halves[0], halves[1]};
}

// Task ids cross shard boundaries, so the allocator is process-wide rather
// than one counter per shard. This keeps the 32-bit ids unique without
// reserving so many bits for a shard number that request-heavy runs wrap.
static std::atomic<uint32_t> next_task_id_value = 1;
thread_local uint32_t current_task_id = 0;

uint32_t next_task_id() noexcept {
    uint32_t id;
    do {
        id = next_task_id_value.fetch_add(1, std::memory_order_relaxed);
    } while (id == 0);
    return id;
}

namespace {

// Smaller than the tracer's defaults because on a reactor thread these come out
// of the shard's memory budget: a --memory=2G two-shard run has 1G a shard.
// Still room for something like half a million events before the ring starts
// evicting.
constexpr size_t info_capacity = 4 * 1024 * 1024;
constexpr size_t debug_capacity = 32 * 1024 * 1024;
constexpr size_t metadata_capacity = 64 * 1024;

struct rpc_connection_record {
    std::string local;
    std::string remote;
    boot_id peer_boot;
    uint32_t peer_shard = 0;
};

// This is intentionally per shard. RPC connections are owned by one reactor
// thread, while the ids themselves are process-wide and therefore allocated
// from the atomic below.
thread_local std::unordered_map<uint64_t, rpc_connection_record> rpc_connections;
std::atomic<uint64_t> next_rpc_connection{1};

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

void ensure_thread_tracer() noexcept {
    ensure_tracer();
}

void trace_task_queue_run_begin(uint32_t scheduling_group) noexcept {
    ensure_tracer();
    TRACEPOINT(tracer::event_level::debug, "task_queue_run_begin",
            "scheduling_group", scheduling_group);
}

void trace_task_queue_run_end() noexcept {
    ensure_tracer();
    TRACEPOINT(tracer::event_level::debug, "task_queue_run_end");
}

void trace_execution_stage(uint32_t task) noexcept {
    ensure_tracer();
    TRACEPOINT(tracer::event_level::debug, "execution_stage", "task", task);
}

void trace_io_queued(uint32_t task, uint64_t io, int32_t fd, uint32_t direction,
        uint32_t priority_class, uint64_t offset, uint64_t length) noexcept {
    ensure_tracer();
    TRACEPOINT(tracer::event_level::debug, "io_queued", "task", task, "io", io,
            "fd", fd, "direction", direction, "priority_class", priority_class,
            "offset", offset, "length", length);
}

void trace_io_dispatched(uint64_t io) noexcept {
    ensure_tracer();
    TRACEPOINT(tracer::event_level::debug, "io_dispatched", "io", io);
}

void trace_io_completed(uint32_t task, uint64_t io) noexcept {
    ensure_tracer();
    TRACEPOINT(tracer::event_level::debug, "io_completed", "task", task, "io", io);
}

void trace_io_cancelled(uint64_t io) noexcept {
    ensure_tracer();
    TRACEPOINT(tracer::event_level::debug, "io_cancelled", "io", io);
}

uint64_t next_rpc_connection_id() noexcept {
    return next_rpc_connection.fetch_add(1, std::memory_order_relaxed);
}

void trace_rpc_connection_open(uint64_t connection, std::string_view local,
        std::string_view remote, boot_id peer_boot, uint32_t peer_shard) noexcept {
    try {
        rpc_connections.insert_or_assign(connection, rpc_connection_record{
                std::string(local), std::string(remote), peer_boot, peer_shard});
        ensure_tracer();
        TRACEPOINT(tracer::event_level::info, "rpc_connection_open",
                "connection", connection, "local", local, "remote", remote,
                "peer_boot_msb", peer_boot.msb, "peer_boot_lsb", peer_boot.lsb,
                "peer_shard", peer_shard);
    } catch (...) {
        // Tracing must never change RPC connection establishment semantics.
    }
}

void trace_rpc_connection_close(uint64_t connection, boot_id peer_boot,
        uint32_t peer_shard) noexcept {
    try {
        rpc_connections.erase(connection);
        ensure_tracer();
        TRACEPOINT(tracer::event_level::info, "rpc_connection_close",
                "connection", connection,
                "peer_boot_msb", peer_boot.msb, "peer_boot_lsb", peer_boot.lsb,
                "peer_shard", peer_shard);
    } catch (...) {
    }
}

void trace_rpc_message_sent(uint64_t connection, uint64_t sequence, uint32_t task) noexcept {
    ensure_tracer();
    TRACEPOINT(tracer::event_level::debug, "rpc_message_sent",
            "connection", connection, "sequence", sequence, "task", task);
}

void trace_rpc_message_received(uint64_t connection, uint64_t sequence) noexcept {
    ensure_tracer();
    TRACEPOINT(tracer::event_level::debug, "rpc_message_received",
            "connection", connection, "sequence", sequence);
}

void trace_rpc_reply_sent(uint64_t connection, uint64_t sequence, int64_t msg_id,
        uint32_t task) noexcept {
    ensure_tracer();
    TRACEPOINT(tracer::event_level::debug, "rpc_reply_sent",
            "connection", connection, "sequence", sequence, "msg_id", msg_id, "task", task);
}

void trace_rpc_reply_received(uint64_t connection, uint64_t sequence, int64_t msg_id) noexcept {
    ensure_tracer();
    TRACEPOINT(tracer::event_level::debug, "rpc_reply_received",
            "connection", connection, "sequence", sequence, "msg_id", msg_id);
}

void trace_rpc_connections_snapshot() noexcept {
    try {
        ensure_tracer();
        TRACEPOINT(tracer::event_level::info, "rpc_connections_snapshot_begin");
        for (const auto& [connection, record] : rpc_connections) {
            TRACEPOINT(tracer::event_level::info, "rpc_connection_snapshot_entry",
                    "connection", connection, "local", std::string_view(record.local),
                    "remote", std::string_view(record.remote),
                    "peer_boot_msb", record.peer_boot.msb,
                    "peer_boot_lsb", record.peer_boot.lsb,
                    "peer_shard", record.peer_shard);
        }
        TRACEPOINT(tracer::event_level::info, "rpc_connections_snapshot_end");
    } catch (...) {
    }
}

void trace_rpc_request_handled(uint64_t connection, uint64_t sequence, uint32_t task) noexcept {
    ensure_tracer();
    TRACEPOINT(tracer::event_level::debug, "rpc_request_handled",
            "connection", connection, "sequence", sequence, "task", task);
}

future<bool> set_tracepoints_enabled(bool enabled) {
    // The action runs on shard 0 with every other shard parked in its poll
    // loop, which is the only place this may happen: it rewrites the branch
    // instruction at every tracepoint call site, and a shard executing one of
    // them as it changes is undefined. It touches no seastar state and does
    // not allocate -- it walks the tracepoint table and calls mprotect and
    // memcpy -- which is what a rendezvous action has to be.
    //
    // The stack sampler follows the same switch, and does not need the
    // rendezvous: enabling a perf event is an ioctl on a file descriptor, not
    // a patch of running code. It is set here rather than beside the patching
    // so that a rendezvous that never gathered leaves *nothing* switched.
    return run_at_rendezvous([enabled] { tracer::set_all_tracepoints_enabled(enabled); })
            .then([enabled](bool switched) {
        if (switched) {
            set_stacktrace_sampling_enabled(enabled);
        }
        return switched;
    });
}

uint64_t next_io_id() noexcept {
    // Shard in the top bits and never zero: a viewer merging the shards' files
    // pairs an io_queued with its io_completed by this number alone.
    static thread_local uint64_t next = (uint64_t(this_shard_id()) << 48) | 1;
    return next++;
}

std::vector<trace_snapshot_part> trace_snapshot() {
    ensure_tracer();
    static constexpr const char* level_names[] = {"info", "debug", "metadata"};
    std::vector<trace_snapshot_part> parts;
    for (unsigned level = 0; level < unsigned(tracer::event_level::count); ++level) {
        const auto which = tracer::event_level(level);
        if (which == tracer::event_level::metadata) {
            // Not a part of its own: it goes into every other part, because a
            // record level without it is undecodable. See collect_trace_level().
            continue;
        }
        const auto [first_ns, last_ns] = tracer::local_tracer->group(which).time_range();
        parts.push_back(trace_snapshot_part{
                level, level_names[level], first_ns, last_ns,
                tracer::collect_trace_level(*tracer::local_tracer, which)});
    }
    return parts;
}

std::string trace_build_id() {
    return tracer::executable_build_id();
}

}
