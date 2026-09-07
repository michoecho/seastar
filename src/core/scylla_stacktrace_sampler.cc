/*
 * The stack sampler; see include/seastar/core/scylla_stacktrace_sampler.hh.
 *
 * One perf event and one mmap ring per reactor thread, opened lazily the first
 * time sampling is switched on and kept for the life of the process. The
 * tracepoint call site is here rather than in tracing/tracer.cc only because
 * this is the one place that has anything to say to it; it is still an
 * out-of-line function in libseastar.so, which is what a static key needs --
 * see the header comment on tracing/tracer.hh.
 */

#include <seastar/core/scylla_stacktrace_sampler.hh>

#include <tracing/tracer.hh>
#include <tracing/tracer_control.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/util/log.hh>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <memory>
#include <span>

#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "tracer/tracer.h"

namespace seastar {

namespace {

logger sampler_logger("stacktrace_sampler");

// Whether the shards should be sampling. Written by
// set_stacktrace_sampling_enabled(), read once per poll by every shard.
std::atomic<bool> sampling_wanted{false};

// The data half of the ring, in pages; the metadata page is one more on top.
// 256 KiB holds a couple of thousand samples, which at 100 Hz is minutes of
// slack -- far more than the poll loop will ever need, and small enough not to
// be worth thinking about against a shard's memory budget.
constexpr std::size_t ring_data_pages = 64;

// The kernel's ceiling on a callchain, and what perf_event_max_stack defaults
// to. A walk that goes deeper is truncated by the kernel, not by us.
constexpr std::size_t max_frames = 127;

// Everything at or above this in a callchain is a marker -- PERF_CONTEXT_USER
// and friends -- saying which part of the stack the frames after it come from,
// rather than an address. We asked for user frames only, so the markers carry
// no information and are dropped.
constexpr std::uint64_t context_marker_floor = std::uint64_t(-4095);  // PERF_CONTEXT_MAX

// One sample, as the kernel lays it out for
// sample_type = PERF_SAMPLE_TIME | PERF_SAMPLE_CALLCHAIN. The order of the
// fields in a sample record is the order of the PERF_SAMPLE_* bits, not the
// order they were asked for in, so this is time then the callchain and nothing
// between them.
struct sample_body {
    std::uint64_t time;
    std::uint64_t nr;
    // std::uint64_t ips[nr] follows
};

// The tracepoint. `frames` is the callchain with the context markers taken out,
// innermost first: each is a return address in one of the process's objects,
// which is all a frame-pointer walk can say. Turning one into a function and a
// line needs the objects themselves and is a reader's job -- the same division
// of labour as a srcloc::location, see "Source locations" in
// modules/trace-viewer/README.md.
[[gnu::noinline]] void trace_stacktrace_sample(std::uint32_t shard, std::uint64_t time_ns,
                                               std::span<const std::byte> frames) noexcept {
    TRACEPOINT(tracer::event_level::info, "stacktrace_sample", "shard", shard, "time_ns", time_ns,
               "frames", frames);
}

// One thread's perf event and its ring.
class sampler {
public:
    // Whether this thread has an event at all. A failed open is remembered as
    // such and never retried: perf_event_open fails for reasons that do not go
    // away between two polls (paranoid settings, a container without the
    // capability), and a shard retrying a syscall every poll loop would be far
    // worse than a shard that does not sample.
    bool usable() const noexcept { return _fd >= 0; }
    bool failed() const noexcept { return _tried && _fd < 0; }

    void open() noexcept {
        _tried = true;

        perf_event_attr attr{};
        attr.size = sizeof(attr);
        attr.type = PERF_TYPE_SOFTWARE;
        attr.config = PERF_COUNT_SW_CPU_CLOCK;
        attr.sample_type = PERF_SAMPLE_TIME | PERF_SAMPLE_CALLCHAIN;
        attr.sample_freq = stacktrace_sample_hz;
        attr.freq = 1;
        // Nothing here may sample the kernel: it would need privileges this
        // process does not want, and a kernel frame is not something the viewer
        // has an object for anyway.
        attr.exclude_kernel = 1;
        attr.exclude_hv = 1;
        // Off until the poll loop says otherwise, so opening one costs nothing.
        attr.disabled = 1;
        // The sample's own time, in the domain the tracer's clock_sync records
        // use. Without this a sample would be stamped in CLOCK_MONOTONIC and
        // could not be lined up with anything in the trace.
        attr.use_clockid = 1;
        attr.clockid = CLOCK_REALTIME;

        // pid 0, cpu -1: this thread, wherever it runs. A reactor thread is
        // pinned, but following the thread rather than the cpu is what makes a
        // sample this shard's by construction.
        _fd = int(::syscall(__NR_perf_event_open, &attr, 0, -1, -1, PERF_FLAG_FD_CLOEXEC));
        if (_fd < 0) {
            sampler_logger.warn("perf_event_open failed on shard {}: {}. No stack samples from "
                                "this shard; check /proc/sys/kernel/perf_event_paranoid.",
                                this_shard_id(), std::strerror(errno));
            return;
        }

        const std::size_t page = std::size_t(::sysconf(_SC_PAGESIZE));
        _mapping_size = (ring_data_pages + 1) * page;
        void* const base =
                ::mmap(nullptr, _mapping_size, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);
        if (base == MAP_FAILED) {
            sampler_logger.warn("mmap of the perf ring failed on shard {}: {}", this_shard_id(),
                                std::strerror(errno));
            ::close(_fd);
            _fd = -1;
            return;
        }
        _meta = static_cast<perf_event_mmap_page*>(base);
        _data = static_cast<std::byte*>(base) + _meta->data_offset;
        _data_size = _meta->data_size;
    }

    void set_enabled(bool enabled) noexcept {
        if (!usable() || enabled == _enabled) {
            return;
        }
        // Dropping whatever is in the ring on the way *in* rather than on the
        // way out: a shard that was sampled during an earlier enabled window
        // has records nobody drained, and they would come out stamped with a
        // time from before this window began.
        if (enabled) {
            discard();
        }
        ::ioctl(_fd, enabled ? PERF_EVENT_IOC_ENABLE : PERF_EVENT_IOC_DISABLE, 0);
        _enabled = enabled;
    }

    bool has_records() const noexcept {
        return usable() && __atomic_load_n(&_meta->data_head, __ATOMIC_ACQUIRE) != _meta->data_tail;
    }

    // Drain the ring, emitting one tracepoint per sample. Returns whether
    // anything was there. Allocates nothing: the frames go through a fixed
    // buffer on the way from the ring to the tracepoint.
    bool drain() noexcept {
        if (!usable()) {
            return false;
        }
        const std::uint64_t head = __atomic_load_n(&_meta->data_head, __ATOMIC_ACQUIRE);
        std::uint64_t tail = _meta->data_tail;
        if (head == tail) {
            return false;
        }

        const std::uint32_t shard = this_shard_id();
        while (tail < head) {
            perf_event_header header{};
            copy_out(tail, &header, sizeof(header));
            if (header.size < sizeof(header) || tail + header.size > head) {
                // A record the ring cannot hold whole is a ring we no longer
                // understand; skip to the head rather than walking off.
                tail = head;
                break;
            }
            if (header.type == PERF_RECORD_SAMPLE) {
                emit(tail + sizeof(header), header.size - sizeof(header), shard);
            }
            tail += header.size;
        }

        // Released after the copies above, so the kernel does not overwrite a
        // record while it is being read.
        __atomic_store_n(&_meta->data_tail, tail, __ATOMIC_RELEASE);
        return true;
    }

private:
    // The ring is a circle of _data_size bytes indexed by a counter that never
    // wraps, so a record may straddle the end. Every read goes through here.
    void copy_out(std::uint64_t offset, void* out, std::size_t n) const noexcept {
        const std::size_t start = offset % _data_size;
        const std::size_t first = std::min(n, _data_size - start);
        std::memcpy(out, _data + start, first);
        if (first < n) {
            std::memcpy(static_cast<std::byte*>(out) + first, _data, n - first);
        }
    }

    void emit(std::uint64_t body_at, std::size_t body_size, std::uint32_t shard) noexcept {
        sample_body body{};
        if (body_size < sizeof(body)) {
            return;
        }
        copy_out(body_at, &body, sizeof(body));
        const std::size_t nr = std::min<std::uint64_t>(body.nr, max_frames);
        if (nr * sizeof(std::uint64_t) + sizeof(body) > body_size) {
            return;
        }
        copy_out(body_at + sizeof(body), _frames.data(), nr * sizeof(std::uint64_t));

        // Compacted in place: the markers are always at the front of the run
        // they introduce, so dropping them keeps the frames in order.
        std::size_t kept = 0;
        for (std::size_t i = 0; i < nr; ++i) {
            if (_frames[i] < context_marker_floor) {
                _frames[kept++] = _frames[i];
            }
        }
        if (kept == 0) {
            return;
        }
        trace_stacktrace_sample(
                shard, body.time,
                std::as_bytes(std::span<const std::uint64_t>(_frames.data(), kept)));
    }

    // Everything the kernel has written so far is somebody else's window.
    void discard() noexcept {
        __atomic_store_n(&_meta->data_tail, __atomic_load_n(&_meta->data_head, __ATOMIC_ACQUIRE),
                         __ATOMIC_RELEASE);
    }

    int _fd = -1;
    bool _tried = false;
    bool _enabled = false;
    perf_event_mmap_page* _meta = nullptr;
    std::byte* _data = nullptr;
    std::size_t _data_size = 0;
    std::size_t _mapping_size = 0;
    std::array<std::uint64_t, max_frames> _frames{};
};

// Leaked for the same reason the rings in tracing/tracer.cc are: a reactor is
// still running tasks while thread_locals are being torn down.
thread_local sampler* local_sampler = nullptr;

class stacktrace_sampler_pollfn final : public pollfn {
public:
    virtual bool poll() override {
        const bool wanted = sampling_wanted.load(std::memory_order_relaxed);
        if (wanted != _enabled) {
            _enabled = wanted;
            apply(wanted);
        }
        return _enabled && local_sampler != nullptr && local_sampler->drain();
    }

    virtual bool pure_poll() override {
        return _enabled && local_sampler != nullptr && local_sampler->has_records();
    }

    // A sleeping shard is not on the cpu, and a cpu-clock event does not tick
    // for it -- so there is nothing to stay awake for, even while sampling.
    virtual bool try_enter_interrupt_mode() override { return true; }
    virtual void exit_interrupt_mode() override { }

private:
    void apply(bool enabled) {
        if (local_sampler == nullptr) {
            if (!enabled) {
                return;  // never opened, and not being asked to
            }
            local_sampler = new sampler();
            local_sampler->open();
            // The rings are what the tracepoint writes into, and this poller
            // may well be the first thing on this thread to reach one.
            ensure_thread_tracer();
        }
        local_sampler->set_enabled(enabled);
    }

    bool _enabled = false;
};

}

void set_stacktrace_sampling_enabled(bool enabled) noexcept {
    sampling_wanted.store(enabled, std::memory_order_relaxed);
}

namespace internal {

std::unique_ptr<pollfn> make_stacktrace_sampler_pollfn() {
    return std::make_unique<stacktrace_sampler_pollfn>();
}

}

}
