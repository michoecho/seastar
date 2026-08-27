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
 * Copyright 2019 ScyllaDB
 */

#pragma once

#include <seastar/core/smp-common.hh>

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/util/noncopyable_function.hh>

#include <memory>
#include <optional>
#include <ranges>
#include <tuple>
#include <type_traits>
#include <utility>

/// \file
///
/// The simulator's implementation of cross-shard messaging.
///
/// It offers the same interface as the reactor's \ref smp (in
/// \ref smp-reactor.hh), so that code using `smp::submit_to()` compiles
/// unchanged against either.  The difference is what the interface is built
/// on: instead of inlining down to the reactor's lock-free message queues,
/// every cross-shard call goes through a single out-of-line, type-erased
/// hook, which the simulator defines.  That keeps the reactor's message
/// queues -- and the reactor itself -- out of the object code of anything
/// compiled in this mode.
///
/// The cost is a heap allocation per cross-shard call and the loss of
/// inlining, which is acceptable for code running under simulation.

namespace seastar {

namespace internal {

/// Runs \c func on shard \c t, type-erased.
///
/// The caller has already dealt with the same-shard case and with wrapping
/// the result, so this only has to deliver a `future<>`-returning function to
/// another shard, and resolve the returned future once it has run.  \c func
/// is destroyed on the shard which ran it.
///
/// Defined by the simulator.
future<> smp_submit_to_erased(shard_id t, smp_submit_to_options options,
        noncopyable_function<future<> ()> func);

} // namespace internal

/// A set of cooperating shards.
///
/// The simulator's stand-in for the reactor's \ref smp.  It presents the
/// same interface, but the shards it manages are simulated rather than
/// backed by reactor threads.
class smp {
    static unsigned _shard_count;
    static smp* _this_smp;
public:
    /// The number of shards available in this `smp` instance. Does not change over the lifetime of the instance.
    unsigned shard_count() const { return _shard_count; }

    /// Runs a function on a remote core.
    ///
    /// \param t designates the core to run the function on (may be a remote
    ///          core or the local core).
    /// \param options an \ref smp_submit_to_options that contains options for this call.
    /// \param func a callable to run on core \c t.
    ///          If \c func is a temporary object, its lifetime will be
    ///          extended by moving. This movement and the eventual
    ///          destruction of func are both done in the _calling_ core.
    ///          If \c func is a reference, the caller must guarantee that
    ///          it will survive the call.
    /// \return whatever \c func returns, as a future<> (if \c func does not return a future,
    ///         submit_to() will wrap it in a future<>).
    template <typename Func>
    static futurize_t<std::invoke_result_t<Func>> submit_to(unsigned t, smp_submit_to_options options, Func&& func) noexcept {
        using ret_type = std::invoke_result_t<Func>;
        using futurator = futurize<ret_type>;
        // The tuple representation carries a void result as an empty tuple,
        // so it handles every return type uniformly.
        using tuple_type = typename futurator::tuple_type;
        try {
            if (t == this_shard_id()) {
                if (!is_future<ret_type>::value) {
                    // Non-deferring function, so don't worry about func lifetime
                    return futurator::invoke(std::forward<Func>(func));
                } else if (std::is_lvalue_reference_v<Func>) {
                    // func is an lvalue, so caller worries about its lifetime
                    return futurator::invoke(func);
                } else {
                    // Deferring call on rvalue function, make sure to preserve it across call
                    auto w = std::make_unique<std::decay_t<Func>>(std::move(func));
                    auto ret = futurator::invoke(*w);
                    return ret.finally([w = std::move(w)] {});
                }
            }
            // The hook can only carry a `future<>`, so the result is parked in
            // a holder which the two shards share.  As in the reactor's
            // message queues, the remote shard only stores the result there;
            // the promise behind the returned future belongs to this shard and
            // is only ever touched here, since a promise may not be used from
            // another shard.
            struct result_holder {
                std::optional<tuple_type> value;
                std::exception_ptr ex; // if !value
            };
            auto holder = std::make_unique<result_holder>();
            auto* h = holder.get();
            return internal::smp_submit_to_erased(t, options,
                    [func = std::forward<Func>(func), h] () mutable -> future<> {
                return futurator::invoke(func).then_wrapped([h] (typename futurator::type f) {
                    if (f.failed()) {
                        // FIXME: the exception was allocated on another shard.
                        h->ex = f.get_exception();
                    } else if constexpr (std::is_same_v<tuple_type, std::tuple<>>) {
                        h->value.emplace();
                    } else {
                        h->value.emplace(f.get());
                    }
                });
            }).then_wrapped([holder = std::move(holder)] (future<> f) {
                // Back on the submitting shard: turn the parked result into
                // the future the caller is holding.
                if (f.failed()) {
                    // The call could not be delivered, or was aborted.
                    return futurator::make_exception_future(f.get_exception());
                }
                if (holder->value) {
                    return futurator::from_tuple(std::move(*holder->value));
                }
                return futurator::make_exception_future(std::move(holder->ex));
            });
        } catch (...) {
            // Consistently return a failed future rather than throwing, to simplify callers
            return futurator::make_exception_future(std::current_exception());
        }
    }

    /// Runs a function on a remote core.
    ///
    /// Uses default_smp_service_group() to control resource allocation.
    ///
    /// \param t designates the core to run the function on (may be a remote
    ///          core or the local core).
    /// \param func a callable to run on core \c t.
    /// \return whatever \c func returns, as a future<> (if \c func does not return a future,
    ///         submit_to() will wrap it in a future<>).
    template <typename Func>
    static futurize_t<std::invoke_result_t<Func>> submit_to(unsigned t, Func&& func) noexcept {
        return submit_to(t, smp_submit_to_options{}, std::forward<Func>(func));
    }

    /// Returns a range of all shard IDs.
    std::ranges::range auto all_shards() const noexcept {
        return std::views::iota(0u, _shard_count);
    }

    template <typename Func>
    requires std::is_nothrow_move_constructible_v<Func>
    static futurize_t<std::invoke_result_t<Func>> copy_and_submit_to(unsigned t, smp_submit_to_options options, const Func& func) noexcept {
        static_assert(std::is_nothrow_copy_constructible_v<Func>);
        return submit_to(t, options, Func(func));
    }

    /// Invokes func on all shards.
    ///
    /// \param options the options to forward to the \ref smp::submit_to()
    ///         called behind the scenes.
    /// \param func the function to be invoked on each shard. May return void or
    ///         future<>. Each async invocation will work with a separate copy
    ///         of \c func.
    /// \returns a future that resolves when all async invocations finish.
    template<typename Func>
    requires std::is_nothrow_move_constructible_v<Func>
    static future<> invoke_on_all(smp_submit_to_options options, Func&& func) noexcept {
        static_assert(std::is_same_v<future<>, decltype(func())>, "bad Func signature");
        static_assert(std::is_nothrow_move_constructible_v<Func>);
        return parallel_for_each(std::views::iota(0u, _shard_count), [options, &func] (unsigned id) {
            return smp::copy_and_submit_to(id, options, func);
        });
    }

    /// Invokes func on all shards.
    ///
    /// Passes the default \ref smp_submit_to_options to the
    /// \ref smp::submit_to() called behind the scenes.
    template<typename Func>
    requires std::is_nothrow_move_constructible_v<Func>
    static future<> invoke_on_all(Func&& func) noexcept {
        return invoke_on_all(smp_submit_to_options{}, std::forward<Func>(func));
    }

    /// Invokes func on all other shards.
    ///
    /// \param cpu_id the shard to exclude
    /// \param options the options to forward to the \ref smp::submit_to()
    ///         called behind the scenes.
    /// \param func the function to be invoked on each shard. May return void or
    ///         future<>. Each async invocation will work with a separate copy
    ///         of \c func.
    /// \returns a future that resolves when all async invocations finish.
    template<typename Func>
    requires std::is_nothrow_move_constructible_v<Func>
    static future<> invoke_on_others(unsigned cpu_id, smp_submit_to_options options, Func func) noexcept {
        static_assert(std::is_same_v<future<>, decltype(func())>, "bad Func signature");
        static_assert(std::is_nothrow_move_constructible_v<Func>);
        return parallel_for_each(std::views::iota(0u, _shard_count), [cpu_id, options, func = std::move(func)] (unsigned id) {
            return id != cpu_id ? smp::copy_and_submit_to(id, options, func) : make_ready_future<>();
        });
    }

    /// Invokes func on all other shards.
    ///
    /// Passes the default \ref smp_submit_to_options to the
    /// \ref smp::submit_to() called behind the scenes.
    template<typename Func>
    requires std::is_nothrow_move_constructible_v<Func>
    static future<> invoke_on_others(unsigned cpu_id, Func func) noexcept {
        return invoke_on_others(cpu_id, smp_submit_to_options{}, std::move(func));
    }

    /// Invokes func on all shards but the current one
    ///
    /// \param func the function to be invoked on each shard. May return void or
    ///         future<>. Each async invocation will work with a separate copy
    ///         of \c func.
    /// \returns a future that resolves when all async invocations finish.
    template<typename Func>
    requires std::is_nothrow_move_constructible_v<Func>
    static future<> invoke_on_others(Func func) noexcept {
        return invoke_on_others(this_shard_id(), std::move(func));
    }

    static smp& this_smp() noexcept {
        return *_this_smp;
    }
};

/// Returns the smp object used for cross-shard communications.
inline
smp&
this_smp() noexcept {
    return smp::this_smp();
}

/// Returns the number of shards in the current smp instance.
inline
unsigned
this_smp_shard_count() noexcept {
    return this_smp().shard_count();
}

/// Returns a range of all shard ids in the current smp instance.
inline
std::ranges::range auto
this_smp_all_shards() noexcept {
    return this_smp().all_shards();
}

}
