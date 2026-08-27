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
// A stub alternative implementation of the non-core half of Seastar.
//
// `seastar-core` declares Seastar's core abstractions but does not define
// them; the definitions normally come from the `seastar` library, which
// implements them on top of the reactor.  This file is the skeleton of a
// second implementation -- a deterministic simulator -- which is meant to
// eventually replace `seastar` for testing purposes.
//
// For now it only establishes the boundary: it defines exactly the symbols
// that `seastar_rpc` refers to, so that `seastar_rpc` + `seastar-core` +
// `seastar-simulator` link.  Every definition is a placeholder which calls
// `unimplemented()`; they are to be filled in one by one as the simulator
// grows.
//

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/iostream-impl.hh>
#include <seastar/core/internal/run_in_background.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/preempt.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/task.hh>
#include <seastar/core/timer.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/assert.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>
#include <seastar/util/log-impl.hh>

#include <cstdio>
#include <cstdlib>

namespace seastar {

namespace {

// The single placeholder body shared by every definition below.
[[noreturn]] [[gnu::noinline]]
void unimplemented(const char* what = __builtin_FUNCTION()) {
    std::fprintf(stderr, "seastar-simulator: unimplemented: %s\n", what);
    std::abort();
}

}

//
// Tasks and the scheduler.
//

void schedule(task*) noexcept { unimplemented(); }
void schedule_checked(task*) noexcept { unimplemented(); }
void schedule_urgent(task*) noexcept { unimplemented(); }

future<> yield() noexcept { unimplemented(); }

//
// Per-shard state.
//
// These accessors are out-of-line only in a shared-library build; in a static
// build the headers define them inline.
//

namespace internal {

#ifdef SEASTAR_BUILD_SHARED_LIBS

shard_id* this_shard_id_ptr() noexcept {
    static thread_local shard_id g_this_shard_id;
    return &g_this_shard_id;
}

const preemption_monitor*& get_need_preempt_var() {
    static preemption_monitor bootstrap_preemption_monitor;
    static thread_local const preemption_monitor* g_need_preempt = &bootstrap_preemption_monitor;
    return g_need_preempt;
}

scheduling_group* current_scheduling_group_ptr() noexcept {
    static thread_local scheduling_group sg;
    return &sg;
}

#endif

[[noreturn]]
void assert_fail(const char* msg, const char* file, int line, const char* func) {
    std::fprintf(stderr, "%s:%d: %s: assertion `%s' failed\n", file, line, func, msg);
    std::abort();
}

void run_in_background(future<>) { unimplemented(); }

[[noreturn]] void throw_bad_alloc() { throw std::bad_alloc(); }
[[noreturn]] void throw_sstring_overflow() { throw std::overflow_error("sstring overflow"); }
[[noreturn]] void throw_sstring_out_of_range() { throw std::out_of_range("sstring out of range"); }

}

//
// Futures and promises.
//

future_state_base::future_state_base(current_exception_future_marker) noexcept { unimplemented(); }

future_state_base::future_state_base(nested_exception_marker, future_state_base&&, future_state_base&&) noexcept {
    unimplemented();
}

void future_state_base::ignore() noexcept { unimplemented(); }

void future_state_base::rethrow_exception() && { unimplemented(); }

// The header declares this `extern template`, so the implementation owes the
// instantiation.  The template body is in the header and needs nothing from
// here beyond `future_state_base`'s constructor above.
template
future<void> current_exception_as_future() noexcept;

namespace internal {

promise_base::promise_base(promise_base&&) noexcept { unimplemented(); }
promise_base& promise_base::operator=(promise_base&&) noexcept { unimplemented(); }
void promise_base::clear_on_broken() noexcept { unimplemented(); }
void promise_base::set_to_current_exception() noexcept { unimplemented(); }

void future_base::do_wait() noexcept { unimplemented(); }
void future_base::set_coroutine(task&) noexcept { unimplemented(); }

void report_failed_future(const std::exception_ptr&) noexcept { unimplemented(); }
void report_failed_future(const future_state_base&) noexcept { unimplemented(); }
void report_failed_future(future_state_base::any&&) noexcept { unimplemented(); }

}

//
// Loops.
//

parallel_for_each_state::parallel_for_each_state(size_t) { unimplemented(); }
void parallel_for_each_state::add_future(future<>&&) { unimplemented(); }
future<> parallel_for_each_state::get_future() { unimplemented(); }
void parallel_for_each_state::wait_for_one() noexcept { unimplemented(); }
void parallel_for_each_state::run_and_dispose() noexcept { unimplemented(); }

//
// Semaphores.
//

const char* broken_semaphore::what() const noexcept { return "semaphore broken"; }
const char* semaphore_timed_out::what() const noexcept { return "semaphore timed out"; }
const char* semaphore_aborted::what() const noexcept { return "semaphore aborted"; }

semaphore_timed_out semaphore_default_exception_factory::timeout() noexcept { return semaphore_timed_out(); }
broken_semaphore semaphore_default_exception_factory::broken() noexcept { return broken_semaphore(); }
semaphore_aborted semaphore_default_exception_factory::aborted() noexcept { return semaphore_aborted(); }

//
// Clocks and timers.
//

// Out-of-line only in a shared-library build; otherwise the header defines it
// as an inline variable.
#ifdef SEASTAR_BUILD_SHARED_LIBS
thread_local lowres_clock::time_point lowres_clock::_now;
#endif

// The out-of-line members of `timer<Clock>` are defined by the
// implementation, not by the header, since arming a timer means handing it to
// whatever is driving time.  As in the reactor, they are defined once as a
// template and then instantiated for each clock that is used.

template <typename Clock>
timer<Clock>::~timer() {
    if (_queued) {
        unimplemented("timer::~timer");
    }
}

template <typename Clock>
void timer<Clock>::arm(time_point, std::optional<duration>) noexcept { unimplemented("timer::arm"); }

template <typename Clock>
void timer<Clock>::readd_periodic() noexcept { unimplemented("timer::readd_periodic"); }

template <typename Clock>
bool timer<Clock>::cancel() noexcept { unimplemented("timer::cancel"); }

template class timer<steady_clock_type>;
template class timer<lowres_clock>;

//
// Networking.
//
// `net/api.hh` only forward-declares the types behind the socket handles;
// the implementation is expected to supply them.  The simulator's sockets
// carry no state yet, but the definitions still have to be visible here so
// that the handles' destructors can be compiled.
//

namespace net {

class connected_socket_impl {
public:
    virtual ~connected_socket_impl() = default;
};

class socket_impl {
public:
    virtual ~socket_impl() = default;
};

class server_socket_impl {
public:
    virtual ~server_socket_impl() = default;
};

}

connected_socket::connected_socket(connected_socket&&) noexcept { unimplemented(); }
connected_socket::~connected_socket() { }
input_stream<char> connected_socket::input(connected_socket_input_stream_config) { unimplemented(); }
output_stream<char> connected_socket::output(size_t) { unimplemented(); }
void connected_socket::set_nodelay(bool) { unimplemented(); }
void connected_socket::set_keepalive(bool) { unimplemented(); }
void connected_socket::set_keepalive_parameters(const net::keepalive_params&) { unimplemented(); }
void connected_socket::shutdown_input() { unimplemented(); }
void connected_socket::shutdown_output() { unimplemented(); }

socket::socket(socket&&) noexcept { unimplemented(); }
socket::~socket() { }
future<connected_socket> socket::connect(socket_address, socket_address, transport) { unimplemented(); }
void socket::set_reuseaddr(bool) { unimplemented(); }
void socket::shutdown() { unimplemented(); }

server_socket::server_socket(server_socket&&) noexcept { unimplemented(); }
server_socket::~server_socket() { }
future<accept_result> server_socket::accept() { unimplemented(); }
void server_socket::abort_accept() { unimplemented(); }

server_socket listen(socket_address, listen_options) { unimplemented(); }
socket make_socket() { unimplemented(); }

std::ostream& operator<<(std::ostream&, const socket_address&) { unimplemented(); }

namespace internal {
void add_to_flush_poller(output_stream<char>&) noexcept { unimplemented(); }
}

//
// Cross-shard messaging.
//

unsigned smp::_shard_count = 1;
smp* smp::_this_smp = nullptr;

namespace internal {

future<> smp_submit_to_erased(shard_id, smp_submit_to_options, noncopyable_function<future<> ()>) {
    unimplemented();
}

} // namespace internal

//
// Memory.
//

namespace memory::internal {
#ifdef __cpp_constinit
thread_local constinit volatile int critical_alloc_section = 0;
#else
__thread volatile int critical_alloc_section = 0;
#endif
}

//
// Logging.
//

#ifdef SEASTAR_BUILD_SHARED_LIBS
thread_local bool logger::silent = false;
#endif

void logger::do_log(log_level, log_writer&) { unimplemented(); }

void logger::failed_to_log(std::exception_ptr, fmt::string_view, std::source_location) noexcept {
    unimplemented();
}

void log_exception_trace(log_level) noexcept { unimplemented(); }

namespace internal {
void log_buf::realloc_buffer_and_append(char) noexcept { unimplemented(); }
}

//
// Metrics.
//

namespace metrics {

metric_groups::metric_groups() noexcept { unimplemented(); }
metric_groups::~metric_groups() { }
metric_groups& metric_groups::add_group(const group_name_type&, const std::initializer_list<metric_definition>&) {
    unimplemented();
}

metric_definition::metric_definition(const impl::metric_definition_impl&) noexcept { unimplemented(); }
metric_definition::~metric_definition() { }

namespace impl {

metric_definition_impl::metric_definition_impl(metric_name_type, metric_type, metric_function,
        description, std::vector<label_instance>, std::vector<label>) {
    unimplemented();
}

metric_definition_impl& metric_definition_impl::set_skip_when_empty(bool) noexcept { unimplemented(); }

}

}

}
