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
 * Copyright (C) 2016 ScyllaDB.
 */

#pragma once
#include <atomic>
#include <functional>
#include <utility>

namespace seastar {

namespace internal {

struct preemption_monitor {
    // We preempt when head != tail
    // This happens to match the Linux aio completion ring, so we can have the
    // kernel preempt a task by queuing a completion event to an io_context.
    std::atomic<uint32_t> head;
    std::atomic<uint32_t> tail;
};

inline const preemption_monitor*& get_need_preempt_var() {
    static preemption_monitor bootstrap_preemption_monitor;
    static thread_local const preemption_monitor* g_need_preempt = &bootstrap_preemption_monitor;
    return g_need_preempt;
}

void set_need_preempt_var(const preemption_monitor* pm);

}

#if 0
extern thread_local std::function<bool()> need_preempt_fn;

auto with_preempt_function(std::function<bool()> pf, auto&& fn) {
   auto oldpf = std::exchange(need_preempt_fn, std::move(pf));
   auto x = defer([&] {need_preempt_fn = std::move(oldpf);});
   return fn();
}
#endif

bool need_preempt() noexcept;
void setup_preempt_fd();
void desetup_preempt_fd();
}
