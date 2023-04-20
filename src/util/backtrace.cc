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
 * Copyright 2017 ScyllaDB
 */
#include <seastar/util/backtrace.hh>

#include <link.h>
#include <sys/types.h>
#include <unistd.h>

#include <errno.h>
#include <string.h>

#include <seastar/core/print.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>

namespace seastar {

static int dl_iterate_phdr_callback(struct dl_phdr_info *info, size_t size, void *data)
{
    std::size_t total_size{0};
    for (int i = 0; i < info->dlpi_phnum; i++) {
        const auto hdr = info->dlpi_phdr[i];

        // Only account loadable, executable (text) segments
        if (hdr.p_type == PT_LOAD && (hdr.p_flags & PF_X) == PF_X) {
            total_size += hdr.p_memsz;
        }
    }

    reinterpret_cast<std::vector<shared_object>*>(data)->push_back({info->dlpi_name, info->dlpi_addr, info->dlpi_addr + total_size});

    return 0;
}

static std::vector<shared_object> enumerate_shared_objects() {
    std::vector<shared_object> shared_objects;
    dl_iterate_phdr(dl_iterate_phdr_callback, &shared_objects);

    return shared_objects;
}

static const std::vector<shared_object> shared_objects{enumerate_shared_objects()};
static const shared_object uknown_shared_object{"", 0, std::numeric_limits<uintptr_t>::max()};

bool operator==(const frame& a, const frame& b) noexcept {
    return a.so == b.so && a.addr == b.addr;
}

frame decorate(uintptr_t addr) noexcept {
    // If the shared-objects are not enumerated yet, or the enumeration
    // failed return the addr as-is with a dummy shared-object.
    if (shared_objects.empty()) {
        return {&uknown_shared_object, addr};
    }

    auto it = std::find_if(shared_objects.begin(), shared_objects.end(), [&] (const shared_object& so) {
        return addr >= so.begin && addr < so.end;
    });

    // Unidentified addresses are assumed to originate from the executable.
    auto& so = it == shared_objects.end() ? shared_objects.front() : *it;
    return {&so, addr - so.begin};
}

simple_backtrace current_backtrace_tasklocal() noexcept {
    simple_backtrace::vector_type v;
    backtrace([&] (frame f) {
        if (v.size() < v.capacity()) {
            v.emplace_back(std::move(f));
        }
    });
    return simple_backtrace(std::move(v));
}

size_t simple_backtrace::calculate_hash() const noexcept {
    size_t h = 0;
    for (auto f : _frames) {
        h = ((h << 5) - h) ^ (f.so->begin + f.addr);
    }
    return h;
}

std::ostream& operator<<(std::ostream& out, const frame& f) {
    if (!f.so->name.empty()) {
        out << f.so->name << "+";
    }
    out << format("0x{:x}", f.addr);
    return out;
}

std::ostream& operator<<(std::ostream& out, const simple_backtrace& b) {
    char delim[2] = {'\0', '\0'};
    for (auto f : b._frames) {
        out << delim << f;
        delim[0] = b.delimeter();
    }
    return out;
}

std::ostream& operator<<(std::ostream& out, const tasktrace& b) {
    out << b._main;
    for (auto&& e : b._prev) {
        out << "\n   --------";
        std::visit(make_visitor([&] (const shared_backtrace& sb) {
            out << '\n' << sb;
        }, [&] (const task_entry& f) {
            out << "\n   " << f;
        }), e);
    }
    return out;
}

std::ostream& operator<<(std::ostream& out, const task_entry& e) {
    return out << seastar::pretty_type_name(*e._task_type);
}

tasktrace current_tasktrace() noexcept {
    auto main = current_backtrace_tasklocal();

    tasktrace::vector_type prev;
    size_t hash = 0;
    if (local_engine && g_current_context) {
        task* tsk = nullptr;

        thread_context* thread = thread_impl::get();
        if (thread) {
            tsk = thread->waiting_task();
        } else {
            tsk = local_engine->current_task();
        }

        while (tsk && prev.size() < prev.max_size()) {
            shared_backtrace bt = tsk->get_backtrace();
            hash *= 31;
            if (bt) {
                hash ^= bt->hash();
                prev.push_back(bt);
            } else {
                const std::type_info& ti = typeid(*tsk);
                prev.push_back(task_entry(ti));
                hash ^= ti.hash_code();
            }
            tsk = tsk->waiting_task();
        }
    }

    return tasktrace(std::move(main), std::move(prev), hash, current_scheduling_group());
}

saved_backtrace current_backtrace() noexcept {
    return current_tasktrace();
}

tasktrace::tasktrace(simple_backtrace main, tasktrace::vector_type prev, size_t prev_hash, scheduling_group sg) noexcept
    : _main(std::move(main))
    , _prev(std::move(prev))
    , _sg(sg)
    , _hash(_main.hash() * 31 ^ prev_hash)
{ }

bool tasktrace::operator==(const tasktrace& o) const noexcept {
    return _hash == o._hash && _main == o._main && _prev == o._prev;
}

tasktrace::~tasktrace() {}


bool need_preempt_default() noexcept {
#ifndef SEASTAR_DEBUG
    // prevent compiler from eliminating loads in a loop
    std::atomic_signal_fence(std::memory_order_seq_cst);
    auto np = internal::get_need_preempt_var();
    // We aren't reading anything from the ring, so we don't need
    // any barriers.
    auto head = np->head.load(std::memory_order_relaxed);
    auto tail = np->tail.load(std::memory_order_relaxed);
    // Possible optimization: read head and tail in a single 64-bit load,
    // and find a funky way to compare the two 32-bit halves.
    return __builtin_expect(head != tail, false);
#else
    return true;
#endif
}

thread_local int preempt_fd = -1;
thread_local char* preempt_mmap = nullptr;
thread_local uint64_t preempt_sz = 0;

void setup_preempt_fd() {
#if 0
    std::string filepath = fmt::format("{}.{}.preempt", getpid(), this_shard_id());
    preempt_fd = open(filepath.c_str(), O_RDWR | O_CREAT, (mode_t)0600);
    if (preempt_fd == -1) {
        perror(fmt::format("Error opening file for writing: {}", filepath).c_str());
        exit(EXIT_FAILURE);
    }
    if (ftruncate(preempt_fd, 1<<30) == -1) {
        perror(fmt::format("ftruncate: {}", filepath).c_str());
        exit(EXIT_FAILURE);
    }
    preempt_mmap = (char*)mmap(0, 1 << 30, PROT_WRITE, MAP_SHARED, preempt_fd, 0);
#else
    std::string filepath = "bad.preempt";
    preempt_fd = open(filepath.c_str(), O_RDONLY, (mode_t)0600);
    preempt_mmap = (char*)mmap(0, 1 << 30, PROT_READ, MAP_SHARED, preempt_fd, 0);
#endif
    if (!preempt_mmap) {
        perror(fmt::format("mmap: {}", filepath).c_str());
        exit(EXIT_FAILURE);
    }
}

void desetup_preempt_fd() {
    munmap(preempt_mmap, 1 << 30);
    close(preempt_fd);
    preempt_fd = -1;
    preempt_sz = 0;
    preempt_mmap = nullptr;
    std::string filepath = fmt::format("{}.{}.preempt", getpid(), this_shard_id());
    unlink(filepath.c_str());
}

void emit(bool x) noexcept {
    preempt_mmap[8 + preempt_sz++] = x;
    *reinterpret_cast<uint64_t*>(preempt_mmap) = preempt_sz;
}

bool need_preempt() noexcept {
    bool yes = need_preempt_default();
    if (preempt_fd >= 0) {
#if 0
        emit(yes);
#else
        if (preempt_sz < *reinterpret_cast<uint64_t*>(preempt_mmap)) {
            yes = preempt_mmap[8 + preempt_sz++];
        } else {
            yes = true;
        }
#endif
    }
    return yes;
}

thread_local std::function<bool()> need_preempt_fn = need_preempt_default;

} // namespace seastar
