/*
 * Switching the tracepoints of core/scylla_tracer.hh on and off.
 *
 * A header of its own because scylla_tracer.hh is included by task.hh, and so
 * by very nearly every translation unit in Seastar and Scylla: it may not
 * include future.hh -- which includes task.hh right back -- and it is not
 * where a rebuild of everything wants to be triggered from. Only the handful
 * of callers that actually switch tracing on include this.
 */

#pragma once

#include <seastar/core/future.hh>

namespace seastar {

/// Turn every tracepoint in the process on or off.
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

}
