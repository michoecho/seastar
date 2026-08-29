/*
 * The rendezvous point; see include/seastar/core/rendezvous.hh.
 *
 * Two halves that talk through one atomic flag:
 *
 *   - the poller, which every shard runs as the last thing in its poll loop.
 *     While the flag is clear it does nothing at all -- not even take the
 *     barrier's mutex, which is a process-wide lock every shard would
 *     otherwise be hammering once per poll.
 *
 *   - run_at_rendezvous(), on shard 0, which parks a request, raises the flag
 *     and waits for shard 0's own poller to work through it.
 *
 * The flag is also what keeps the shards awake: a reactor with nothing to do
 * sleeps in the kernel and stops polling, so a phase opened while the others
 * are idle would time out forever. try_enter_interrupt_mode() refuses to sleep
 * while the flag is up, and the request pokes every shard once on the way in
 * so that a shard already asleep comes back round the loop to see it.
 */

#include <seastar/core/rendezvous.hh>

#include <atomic>
#include <chrono>
#include <memory>
#include <utility>

#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/smp.hh>

#include "utils/barrier.h"

namespace seastar {

namespace {

using clock = std::chrono::steady_clock;

// How long a phase waits for the other shards before giving up on this
// attempt, how long before it is tried again, and how many attempts there are
// in all. A shard is expected to come round its poll loop far inside the first
// of these; the retries are for the shard that happened to be in the middle of
// something long.
constexpr auto phase_timeout = std::chrono::milliseconds(10);
constexpr auto retry_gap = std::chrono::milliseconds(200);
constexpr unsigned max_attempts = 5;

// One barrier for the process, owned by shard 0 and joined by all the others.
utils::barrier the_barrier;

// Whether a request is in flight. The only thing the other shards read, and
// the reason their poll costs a relaxed load rather than a lock.
std::atomic<bool> rendezvous_requested{false};

// The request itself. Touched by shard 0 alone -- it is written before the
// flag goes up and read after, both on shard 0 -- so it needs no atomicity of
// its own.
struct request {
    noncopyable_function<void()> action;
    promise<bool> done;
    unsigned attempts_left = max_attempts;
    // When the next attempt may be made. The first is due immediately.
    clock::time_point next_attempt = clock::time_point::min();
};
std::unique_ptr<request> pending;

// Serialises callers, so `pending` is never overwritten by a second request
// and the actions never interleave.
semaphore& gate() {
    static semaphore sem(1);
    return sem;
}

// Shard 0's half of one poll: at most one attempt, and only when one is due.
bool poll_owner() {
    if (!pending || clock::now() < pending->next_attempt) {
        return false;
    }

    // Everyone else, since shard 0 is the owner and is right here.
    const bool filled = the_barrier.start_barrier(
            smp::count - 1, clock::now() + phase_timeout,
            [] { pending->action(); });

    if (!filled && --pending->attempts_left > 0) {
        pending->next_attempt = clock::now() + retry_gap;
        return true;
    }

    // Done either way: the flag drops first, so the other shards stop polling
    // into the barrier before anything waits on the result.
    std::unique_ptr<request> finished = std::move(pending);
    rendezvous_requested.store(false, std::memory_order_release);
    finished->done.set_value(filled);
    return true;
}

class rendezvous_pollfn final : public pollfn {
public:
    virtual bool poll() override {
        if (!rendezvous_requested.load(std::memory_order_acquire)) {
            return false;
        }
        if (this_shard_id() == 0) {
            return poll_owner();
        }
        // Parks here for as long as the phase lasts, which is the whole point:
        // this shard is running nothing while shard 0 rewrites code. A call
        // that lands outside a phase -- before it opens, or after it filled --
        // returns at once.
        the_barrier.try_barrier();
        return true;
    }

    virtual bool pure_poll() override {
        return rendezvous_requested.load(std::memory_order_acquire);
    }

    virtual bool try_enter_interrupt_mode() override {
        // A sleeping shard does not poll, and a phase nobody arrives at times
        // out. Stay awake for as long as a request is in flight.
        return !rendezvous_requested.load(std::memory_order_acquire);
    }

    virtual void exit_interrupt_mode() override { }
};

}

namespace internal {

std::unique_ptr<pollfn> make_rendezvous_pollfn() {
    return std::make_unique<rendezvous_pollfn>();
}

}

future<bool> run_at_rendezvous(noncopyable_function<void()> action) {
    assert(this_shard_id() == 0 && "run_at_rendezvous() is shard 0's");

    const auto units = co_await get_units(gate(), 1);

    auto req = std::make_unique<request>();
    req->action = std::move(action);
    future<bool> result = req->done.get_future();
    // Published before the flag goes up: the flag is what makes shard 0's next
    // poll look at it.
    pending = std::move(req);
    rendezvous_requested.store(true, std::memory_order_release);

    // Wake every shard, so that one already asleep comes round its poll loop
    // and reaches the poller above -- which will then keep it awake.
    //
    // Not on the critical path: shard 0's poller runs while this is suspended,
    // and a measured request is usually finished before this comes back.
    co_await smp::invoke_on_all([] { });

    co_return co_await std::move(result);
}

}
