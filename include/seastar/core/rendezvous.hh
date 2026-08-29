/*
 * A rendezvous point in the reactor's poll loop, for work that may not run
 * while another shard is looking.
 *
 * Rewriting instructions is the case this exists for: modules/static_keys
 * patches a live branch site, and a shard executing that very instruction
 * while it changes is undefined in a way no amount of care at the patching end
 * can fix. What makes it safe is knowing where the other shards are, and the
 * poll loop is the one place a reactor is known not to be inside anybody's
 * code.
 *
 * The mechanism is a utils::barrier (modules/utils in the cpp_template
 * playground) shared by every shard. Shard 0 owns it: it opens a phase, every
 * other shard walks into it from its poll loop, and the completion -- the
 * actual dangerous work -- runs on shard 0 with all of them parked. See
 * modules/utils/include/utils/barrier.h for why the completion runs on the
 * owner rather than on whichever participant arrived last.
 *
 * A shard that is busy with a long task does not reach its poll loop, so a
 * phase can miss its count; the phase has a deadline for exactly that, and the
 * request is retried a few times before it is given up on. A rendezvous is
 * best-effort and says so in its result.
 */

#pragma once

#include <memory>

#include <seastar/core/future.hh>
#include <seastar/core/internal/poll.hh>
#include <seastar/util/noncopyable_function.hh>

namespace seastar {

/// Run `action` on shard 0 with every other shard parked in its poll loop.
///
/// Callable on shard 0 only -- the barrier's owner is shard 0, and the action
/// runs there. Calls are queued against a semaphore, so only one rendezvous is
/// ever in flight and `action` never runs concurrently with another one.
///
/// Resolves to true if the phase filled and `action` ran, and false if it kept
/// missing its count until the retries ran out -- in which case `action` did
/// not run at all. It is never run partially: the barrier either has every
/// shard or it has none of them.
///
/// `action` runs on the reactor's poll loop, outside any task, with every
/// other shard blocked. Keep it short, and keep it to things that do not
/// allocate, log, or touch another shard.
future<bool> run_at_rendezvous(noncopyable_function<void()> action);

namespace internal {

/// The poll-loop half, registered by the reactor as its last poller. Not part
/// of the API; declared here because reactor.cc is where pollers are made.
std::unique_ptr<pollfn> make_rendezvous_pollfn();

}

}
