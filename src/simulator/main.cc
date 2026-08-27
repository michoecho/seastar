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
 * Copyright (C) 2026 ScyllaDB
 */

//
// A placeholder entry point for `seastar_simulator_link_test`.
//
// The executable exists only so that the link of `seastar_rpc` against
// `seastar-simulator` is exercised by the build: linking a shared library
// does not resolve its undefined symbols, so it takes a program which pulls
// in the RPC code to establish that the simulator covers it.
//
// It does nothing, and is meant to do nothing -- every simulator entry point
// aborts, so anything which actually reached the RPC layer would die on the
// first stub it hit.  Building and loading it is the whole test.
//

#include <seastar/rpc/rpc.hh>

int main() {
    return 0;
}
