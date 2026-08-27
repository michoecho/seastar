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

/// \file
///
/// Cross-shard messaging.
///
/// The interface -- `smp::submit_to()` and friends -- has more than one
/// implementation behind it, and this header selects between them.  Code
/// which includes it gets the same interface either way, and so compiles
/// unchanged against both:
///
///  - by default, the reactor's implementation, in \ref smp-reactor.hh,
///    where shards exchange work items over lock-free queues;
///  - with `SEASTAR_SIMULATOR` defined, the simulator's implementation, in
///    \ref smp-simulator.hh, which routes cross-shard calls through a single
///    out-of-line hook and so keeps the reactor out of the picture.
///
/// The two are mutually exclusive: `SEASTAR_SIMULATOR` is a property of the
/// whole build, and everything linked together must agree on it.

#include <seastar/core/smp-common.hh>

#ifdef SEASTAR_SIMULATOR
#include <seastar/core/smp-simulator.hh>
#else
#include <seastar/core/smp-reactor.hh>
#endif
