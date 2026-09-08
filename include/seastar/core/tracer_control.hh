/*
 * Switching the tracepoints of core/tracer.hh -- and of whatever application
 * events were compiled beside them -- on and off.
 *
 * A header of its own because tracer.hh is included by task.hh, and so by very
 * nearly every translation unit in Seastar and in what is built on it: it may
 * not include future.hh -- which includes task.hh right back -- and it is not
 * where a rebuild of everything wants to be triggered from. Only the handful
 * of callers that actually switch tracing on include this.
 */

#pragma once

#include <seastar/core/future.hh>

namespace seastar {

/// Turn every tracepoint in the process on or off.
///
/// Every tracepoint: the switch walks the tracepoint table of every loaded
/// object, so an application's own call sites, in its own executable, follow
/// Seastar's.
///
/// Tracepoints start *disabled*, so nothing is recorded until this has been
/// called with true. Switching one is a patch of the branch instruction at
/// each of its call sites, which is why this is a future and not a setter: the
/// rewrite happens at a rendezvous where every shard is parked in its poll
/// loop, and shard 0 does it there. See core/rendezvous.hh.
///
/// Callable on shard 0 only. Resolves to false if the rendezvous never
/// gathered every shard, in which case nothing was switched and the
/// tracepoints are as they were.
future<bool> set_tracepoints_enabled(bool enabled);

/// Install this thread's trace rings if it has none yet.
///
/// Every out-of-line tracepoint hook does this for itself; it is exposed for
/// direct tracepoint call sites -- including an application's -- and for the
/// stack sampler, whose poller may be the first thing on a reactor thread to
/// write a record. Constructing the rings walks the loaded objects and writes
/// the metadata prologue, so it cannot happen at static-init time.
void ensure_thread_tracer() noexcept;

}
