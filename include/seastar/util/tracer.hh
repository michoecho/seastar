#pragma once

#include <vector>
#include <cstdint>
#include <cstring>
#include <string>
#include <span>
#include <array>
#include <seastar/core/byteorder.hh>
#include <list>

using byte = std::byte;

namespace seastar {

uint64_t static inline rdtsc() {
    uint64_t rax, rdx;
    asm volatile ( "rdtsc" : "=a" (rax), "=d" (rdx) );
    return (uint64_t)(( rdx << 32 ) + rax);
}

struct tracer {
    enum class event_level {
        INFO,
        DEBUG,
        COUNT,
    };

    static constexpr size_t buffer_size = (128 * 1024);

    struct buffer_group {
        struct buffer : std::vector<std::byte> {
            using std::vector<std::byte>::vector;
        };

        buffer _current;
        size_t _cur_pos = 0;
        size_t _used;
        size_t _capacity;
        std::list<buffer> _old;

        [[gnu::noinline]]
        void rotate() {
            _current.resize(_cur_pos);
            _used += _current.capacity();
            _old.push_back(std::move(_current));
            while (_used > _capacity) {
                _used -= _old.front().capacity();
                _current = std::move(_old.front());
                _old.pop_front();
            }
            _current.resize(buffer_size);
            _cur_pos = 0;
        }

        [[gnu::always_inline]]
        std::byte* write(size_t n) {
            if (_current.size() - _cur_pos < n) [[unlikely]] {
                rotate();
            }
            auto result = &_current[_cur_pos];
            _cur_pos += n;
            return result;
        }

        buffer_group(size_t capacity) : _capacity(capacity) {
            for (_used = 0; _used < _capacity; _used += buffer_size) {
                _old.push_back(buffer());
                _old.back().reserve(buffer_size);
            }
            _current.resize(buffer_size);
        }
    };

    [[gnu::always_inline]]
    std::byte* write(event_level level, size_t n) {
        return _groups[static_cast<size_t>(level)].write(n);
    }

    std::array<buffer_group, static_cast<size_t>(event_level::COUNT)> _groups{4*1024*1024, 64*1024*1024};
};

struct constexpr_string {
    const char* s;
    consteval constexpr_string(const char* ss) : s(ss) {
    }
    constexpr std::string val() const noexcept {
        return std::string(s);
    }
};

template <typename T> constexpr std::string type_to_sig() {
    if constexpr (sizeof(T) <= 8) {
        return "unknown64";
    } else {
        return "unknown128";
    }
}
template <> constexpr std::string type_to_sig<uint64_t>() { return "u64"; }
template <> constexpr std::string type_to_sig<int64_t>() { return "i64"; }
template <> constexpr std::string type_to_sig<uint32_t>() { return "u32"; }
template <> constexpr std::string type_to_sig<int32_t>() { return "i32"; }
template <> constexpr std::string type_to_sig<uint16_t>() { return "u16"; }
template <> constexpr std::string type_to_sig<int16_t>() { return "i16"; }
template <> constexpr std::string type_to_sig<uint8_t>() { return "u8"; }
template <> constexpr std::string type_to_sig<int8_t>() { return "i8"; }
template <> constexpr std::string type_to_sig<bool>() { return "bool"; }
template <> constexpr std::string type_to_sig<std::span<const byte>>() { return "bytes"; }
template <> constexpr std::string type_to_sig<void const*>() { return "ptr"; }

template <typename... Args>
constexpr std::string compute_signature() {
    return {};
}

template <typename T, typename... Args>
constexpr std::string compute_signature(T*, Args&&... args) {
    std::string prefix = type_to_sig<std::remove_cvref_t<T>>();
    if constexpr (sizeof...(Args) == 0) {
        return prefix;
    } else {
        return prefix + "," + compute_signature(std::forward<Args>(args)...);
    }
}

template <size_t N>
constexpr auto arrayify_constexpr_string(std::string result) {
    std::array<char, N+1> arr = {};
    int i = 0;
    for (auto c : result) {
        arr[i++] = c;
    }
    return arr;
}
#define COMPUTE_SIGNATURE(...) arrayify_constexpr_string<compute_signature(__VA_ARGS__).size()>(compute_signature(__VA_ARGS__))

template<typename T>
constexpr size_t compute_unknown_size_impl() {
    if constexpr (sizeof(T) <= 8) {
        return 8;
    } else {
        return 16;
    }
}

template<typename T>
requires (!std::integral<T>)
constexpr size_t compute_size_impl(const T& x) {
    return compute_unknown_size_impl<T>();
}
template<std::integral T>
constexpr size_t compute_size_impl(const T& x) { return sizeof(x); }
constexpr size_t compute_size_impl(void const* const& x) { return sizeof(x); }
constexpr size_t compute_size_impl(const std::span<const std::byte>& x) { return x.size() + sizeof(uint16_t); }
template <typename... Args>
size_t compute_size(const Args&... args) {
    return (compute_size_impl(args) + ... + 0);
}

template <typename From>
void write_int(std::byte*& out, const From& x)
requires std::is_trivially_copyable_v<From> {
    memcpy(out, &x, sizeof(x));
    out += sizeof(x);
}

template<typename T>
requires (!std::integral<T>)
inline void serialize_tracepoint_impl(std::byte*& out, const T& x) {
    constexpr size_t sz = compute_unknown_size_impl<T>();
    memcpy(out, reinterpret_cast<const void*>(&x), std::min<size_t>(sizeof(x), sz));
    out += sz;
}
template<std::integral T>
inline void serialize_tracepoint_impl(std::byte*& out, const T& x) { write_int(out, x); }
inline void serialize_tracepoint_impl(std::byte*& out, void const* const& x) { write_int(out, uint64_t(x)); }
inline void serialize_tracepoint_impl(std::byte*& out, const std::span<const std::byte>& x) {
    write_int(out, uint16_t(x.size()));
    memcpy(out, x.data(), x.size());
    out += x.size();
}
template <typename... Args>
void serialize_tracepoint(std::byte*& out, const Args&... args) {
    (serialize_tracepoint_impl(out, args), ...);
}

typedef struct {
    const char* name;
    const char* file;
    int line;
    int level;
    const char* function;
    const char* signature;
} tracepoint_entry;

extern __thread tracer* local_tracer;

#define COMBINE_TOKENS(a, b) COMBINE_TOKENS_IMPL(a, b)
#define COMBINE_TOKENS_IMPL(a, b) a##b

#define NARGS(...) NARGS_IMPL(__VA_ARGS__, 20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0)
#define NARGS_IMPL(_0,_1,_2,_3,_4,_5,_6,_7,_8,_9,_10,_11,_12,_13,_14,_15,_16,_17,_18,_19,_20, N, ...) N

#define SIG_0()
#define SIG_1(v) (std::remove_cvref_t<decltype(v)>*)(nullptr)
#define SIG_2(v, ...) (std::remove_cvref_t<decltype(v)>*)(nullptr), SIG_1(__VA_ARGS__)
#define SIG_3(v, ...) (std::remove_cvref_t<decltype(v)>*)(nullptr), SIG_2(__VA_ARGS__)
#define SIG_4(v, ...) (std::remove_cvref_t<decltype(v)>*)(nullptr), SIG_3(__VA_ARGS__)
#define SIG_5(v, ...) (std::remove_cvref_t<decltype(v)>*)(nullptr), SIG_4(__VA_ARGS__)
#define SIG_6(v, ...) (std::remove_cvref_t<decltype(v)>*)(nullptr), SIG_5(__VA_ARGS__)
#define SIG_7(v, ...) (std::remove_cvref_t<decltype(v)>*)(nullptr), SIG_6(__VA_ARGS__)
#define SIG_8(v, ...) (std::remove_cvref_t<decltype(v)>*)(nullptr), SIG_7(__VA_ARGS__)
#define SIG_9(v, ...) (std::remove_cvref_t<decltype(v)>*)(nullptr), SIG_8(__VA_ARGS__)
#define SIG_10(v, ...) (std::remove_cvref_t<decltype(v)>*)(nullptr), SIG_9(__VA_ARGS__)
#define SIG_11(v, ...) (std::remove_cvref_t<decltype(v)>*)(nullptr), SIG_10(__VA_ARGS__)
#define SIG_12(v, ...) (std::remove_cvref_t<decltype(v)>*)(nullptr), SIG_11(__VA_ARGS__)
#define SIG_13(v, ...) (std::remove_cvref_t<decltype(v)>*)(nullptr), SIG_12(__VA_ARGS__)
#define SIG_N(N, ...) COMBINE_TOKENS(SIG_, N)(__VA_ARGS__)
#define SIG(...) SIG_N(NARGS(0, ##__VA_ARGS__), ##__VA_ARGS__)

#define TRACEPOINT(eventlevel, name, loglevel, ...) { \
    using namespace seastar; \
    static constexpr auto sig __attribute__((section("tracepoint_signatures"), used)) = \
        COMPUTE_SIGNATURE(SIG(__VA_ARGS__)); \
    static constexpr char namearr[] __attribute__((section("tracepoint_names"), used)) = name; \
    static constexpr char filearr[] __attribute__((section("tracepoint_files"), used)) = __FILE__; \
    static constexpr tracepoint_entry tp __attribute__((section("tracepoints"), used)) = { \
        namearr, filearr, __LINE__, int(loglevel), __PRETTY_FUNCTION__, sig.data() \
    }; \
    size_t sz = compute_size(__VA_ARGS__); \
    auto out = local_tracer->write(eventlevel, sz + 16); \
    seastar::write_le<uintptr_t>(reinterpret_cast<char*>(out), reinterpret_cast<uintptr_t>(&tp)); \
    out += sizeof(uintptr_t); \
    seastar::write_le<uint64_t>(reinterpret_cast<char*>(out), rdtsc()); \
    out += sizeof(uint64_t); \
    serialize_tracepoint(out __VA_OPT__(,) __VA_ARGS__); \
}

int tracepoint_decoder_main(int argc, char** argv);

} // namespace seastar
