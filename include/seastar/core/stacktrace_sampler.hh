/*
 * Periodic stack sampling on every shard, emitted as tracepoints.
 *
 * Each reactor thread opens a perf_event_open() software cpu-clock event on
 * *itself*, sampling at stacktrace_sample_hz with PERF_SAMPLE_CALLCHAIN. The
 * kernel walks the user stack by frame pointer -- which is why the build must
 * carry -fno-omit-frame-pointer -- and drops a record into a per-thread mmap
 * ring. There is no stack copying (PERF_SAMPLE_STACK_USER) and no DWARF
 * unwinding: a frame-pointer walk is a handful of loads in the interrupt and
 * costs the traced process nothing it can feel.
 *
 * The ring is drained from the reactor's poll loop, right beside the rendezvous
 * poller -- see core/rendezvous.hh -- because that is where a shard is known to
 * be between tasks. Each sample the drain finds becomes one `stacktrace_sample`
 * tracepoint carrying the shard, the sample's own timestamp, and the frames as
 * a run of addresses.
 *
 * The timestamp is the *kernel's*, not the tracepoint's: a record's own header
 * is an rdtsc taken when the drain ran, which is up to a poll period after the
 * interrupt. The event is opened with clockid CLOCK_REALTIME so that the
 * sample time is in the very domain the tracer's clock_sync records pair ticks
 * with, and a viewer can put a sample back among the tasks it interrupted
 * without calibrating anything of its own.
 *
 * Sampling follows the tracepoints: it is off until set_tracepoints_enabled()
 * turns it on, so a node nobody asked to trace does not run a perf event.
 */

#pragma once

#include <memory>

#include <seastar/core/internal/poll.hh>

namespace seastar {

/// How often each shard is interrupted for a stack sample.
inline constexpr unsigned stacktrace_sample_hz = 100;

/// Ask every shard's sampler to start or stop at its next poll.
///
/// Callable from anywhere: it is one relaxed store, and each shard picks the
/// change up itself -- a perf event may only be enabled from the thread it was
/// opened on, so this cannot do the ioctl on anyone else's behalf.
void set_stacktrace_sampling_enabled(bool enabled) noexcept;

namespace internal {

/// The poll-loop half, registered by the reactor. Declared here because
/// reactor.cc is where pollers are made; see make_rendezvous_pollfn().
std::unique_ptr<pollfn> make_stacktrace_sampler_pollfn();

}

}
