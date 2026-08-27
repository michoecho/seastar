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

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sstring.hh>

#include <optional>

/// \file
///
/// The parts of the cross-shard messaging interface which do not depend on
/// how shards actually talk to each other: service groups, the options and
/// the clock used by \ref smp::submit_to().
///
/// The `smp` class itself is implementation-specific, and lives in one of
/// the variant headers which \ref smp.hh selects between.

namespace seastar {

class smp_service_group;

namespace internal {

unsigned smp_service_group_id(smp_service_group ssg) noexcept;

}

/// Configuration for smp_service_group objects.
///
/// \see create_smp_service_group()
struct smp_service_group_config {
    /// The maximum number of non-local requests that execute on a shard concurrently
    ///
    /// Will be adjusted upwards to allow at least one request per non-local shard.
    unsigned max_nonlocal_requests = 0;
    /// An optional name for this smp group
    ///
    /// If this optional is engaged, timeout exception messages of the group's
    /// semaphores will indicate the group's name.
    std::optional<sstring> group_name;
};

/// A resource controller for cross-shard calls.
///
/// An smp_service_group allows you to limit the concurrency of
/// smp::submit_to() and similar calls. While it's easy to limit
/// the caller's concurrency (for example, by using a semaphore),
/// the concurrency at the remote end can be multiplied by a factor
/// of smp::shard_count()-1, which can be large.
///
/// The class is called a service _group_ because it can be used
/// to group similar calls that share resource usage characteristics,
/// need not be isolated from each other, but do need to be isolated
/// from other groups. Calls in a group should not nest; doing so
/// can result in ABA deadlocks.
///
/// Nested submit_to() calls must form a directed acyclic graph
/// when considering their smp_service_groups as nodes. For example,
/// if a call using ssg1 then invokes another call using ssg2, the
/// internal call may not call again via either ssg1 or ssg2, or it
/// may form a cycle (and risking an ABBA deadlock). Create a
/// new smp_service_group_instead.
class smp_service_group {
    unsigned _id;
#ifdef SEASTAR_DEBUG
    unsigned _version = 0;
#endif
private:
    explicit smp_service_group(unsigned id) noexcept : _id(id) {}

    friend unsigned internal::smp_service_group_id(smp_service_group ssg) noexcept;
    friend smp_service_group default_smp_service_group() noexcept;
    friend future<smp_service_group> create_smp_service_group(smp_service_group_config ssgc) noexcept;
    friend future<> destroy_smp_service_group(smp_service_group) noexcept;
};


inline
unsigned
internal::smp_service_group_id(smp_service_group ssg) noexcept {
    return ssg._id;
}

/// Returns the default smp_service_group. This smp_service_group
/// does not impose any limits on concurrency in the target shard.
/// This makes is deadlock-safe, but can consume unbounded resources,
/// and should therefore only be used when initiator concurrency is
/// very low (e.g. administrative tasks).
smp_service_group default_smp_service_group() noexcept;

/// Creates an smp_service_group with the specified configuration.
///
/// The smp_service_group is global, and after this call completes,
/// the returned value can be used on any shard.
future<smp_service_group> create_smp_service_group(smp_service_group_config ssgc) noexcept;

/// Destroy an smp_service_group.
///
/// Frees all resources used by an smp_service_group. It must not
/// be used again once this function is called.
future<> destroy_smp_service_group(smp_service_group ssg) noexcept;

inline
smp_service_group default_smp_service_group() noexcept {
    return smp_service_group(0);
}

using smp_timeout_clock = lowres_clock;
using smp_service_group_semaphore = basic_semaphore<named_semaphore_exception_factory, smp_timeout_clock>;
using smp_service_group_semaphore_units = semaphore_units<named_semaphore_exception_factory, smp_timeout_clock>;


static constexpr smp_timeout_clock::time_point smp_no_timeout = smp_timeout_clock::time_point::max();

/// Options controlling the behaviour of \ref smp::submit_to().
struct smp_submit_to_options {
    /// Controls resource allocation.
    smp_service_group service_group = default_smp_service_group();
    /// The timeout is relevant only to the time the call spends waiting to be
    /// processed by the remote shard, and *not* to the time it takes to be
    /// executed there.
    smp_timeout_clock::time_point timeout = smp_no_timeout;

    smp_submit_to_options(smp_service_group service_group = default_smp_service_group(), smp_timeout_clock::time_point timeout = smp_no_timeout) noexcept
        : service_group(service_group)
        , timeout(timeout) {
    }
};

void init_default_smp_service_group(shard_id cpu);

smp_service_group_semaphore& get_smp_service_groups_semaphore(unsigned ssg_id, shard_id t) noexcept;
}
