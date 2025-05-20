#include "seastar/util/tracer.hh"
#include "seastar/util/log.hh"
#include <sstream>
#include <iostream>

namespace seastar {

std::string generate_prologue() {
    std::stringstream code;
    code << R"""(
#include <fmt/core.h>
#include <type_traits>
#include <span>
#include <concepts>

template <class T> concept Trivial = std::is_trivial_v<T>;
template <class T> concept TriviallyCopyable = std::is_trivially_copyable_v<T>;

template <TriviallyCopyable To>
To read_unaligned(const std::byte*& p) {
    To dst;
    std::memcpy(&dst, p, sizeof(To));
    p += sizeof(To);
    return dst;
}

struct fmt_hex {
    std::span<const std::byte> v;
    fmt_hex(std::span<const std::byte> v) noexcept : v(v) {}
};

template <>
struct fmt::formatter<fmt_hex> {
    constexpr auto parse(fmt::format_parse_context& ctx) {
        auto it = ctx.begin();
        auto end = ctx.end();

        while (it != end && *it != '}')
            ++it;

        return it;
    }
    template <typename FormatContext>
    auto format(const ::fmt_hex& s, FormatContext& ctx) const {
        auto out = ctx.out();
        const auto& v = s.v;
        for (auto b : v) {
            fmt::format_to(out, "{:02x}", unsigned(b));
        }
        return out;
    }
};
)""";
    return code.str();
}

std::string generate_deserializer(size_t id, const tracepoint_entry& e, size_t max_fileline_size) {
    std::vector<std::string> type_signatures;
    std::stringstream ss(e.signature);
    std::string token;

    while (std::getline(ss, token, ',')) {
        type_signatures.push_back(token);
    }

    std::stringstream code;

    code << "void deserialize_" << id << "(const std::byte*&p) {\n";
    code << "// " << e.signature << "\n";

    code << "    auto ts = read_unaligned<uint64_t>(p);\n";

    // Declare variables
    for (size_t i = 0; i < type_signatures.size(); i++) {
        const std::string& sig = type_signatures[i];
        code << "    ";

        if (sig == "u64") code << "uint64_t";
        else if (sig == "i64") code << "int64_t";
        else if (sig == "u32") code << "uint32_t";
        else if (sig == "i32") code << "int32_t";
        else if (sig == "u16") code << "uint16_t";
        else if (sig == "i16") code << "int16_t";
        else if (sig == "u8") code << "uint8_t";
        else if (sig == "i8") code << "int8_t";
        else if (sig == "bool") code << "bool";
        else if (sig == "bytes") code << "std::span<const std::byte>";
        else if (sig == "ptr") code << "void const*";
        else code << "std::span<const std::byte>";

        code << " arg_" << (i + 1) << ";\n";
    }

    // Deserialize variables
    for (size_t i = 0; i < type_signatures.size(); i++) {
        const std::string& sig = type_signatures[i];
        code << "    ";

        if (sig == "u64") code << "arg_" << (i + 1) << " = read_unaligned<uint64_t>(p);";
        else if (sig == "i64") code << "arg_" << (i + 1) << " = read_unaligned<int64_t>(p);";
        else if (sig == "u32") code << "arg_" << (i + 1) << " = read_unaligned<uint32_t>(p);";
        else if (sig == "i32") code << "arg_" << (i + 1) << " = read_unaligned<int32_t>(p);";
        else if (sig == "u16") code << "arg_" << (i + 1) << " = read_unaligned<uint16_t>(p);";
        else if (sig == "i16") code << "arg_" << (i + 1) << " = read_unaligned<int16_t>(p);";
        else if (sig == "u8") code << "arg_" << (i + 1) << " = read_unaligned<uint8_t>(p);";
        else if (sig == "i8") code << "arg_" << (i + 1) << " = read_unaligned<int8_t>(p);";
        else if (sig == "bool") code << "arg_" << (i + 1) << " = read_unaligned<bool>(p);";
        else if (sig == "bytes") {
            code << "size_t len_" << (i + 1) << " = read_unaligned<uint16_t>(p);\n";
            code << "arg_" << (i + 1) << " = std::span<const std::byte>(p, len_" << (i + 1) << "); p += len_" << (i + 1) << ";\n";
            code << "p += len_" << (i + 1) << ";";
        }
        else if (sig == "unknown128") {
            code << "arg_" << (i + 1) << " = std::span<const std::byte>(p, 16); p += 16;\n";
        }
        else if (sig == "unknown64") {
            code << "arg_" << (i + 1) << " = std::span<const std::byte>(p, 8); p += 8;\n";
        }
        else if (sig == "ptr") code << "arg_" << (i + 1) << " = reinterpret_cast<void const*>(read_unaligned<uintptr_t>(p));";
        else code << "arg_" << (i + 1) << " = \"unknown\";";

        code << "\n";
    }

    // Generate the fmt::print call
    code << "    fmt::println(R\"---({:18} | {:" << max_fileline_size << "} | {:5} | " << e.name << ")---\", ts, \"" << e.file << ":" << e.line << "\", \"" << seastar::log_level(e.level) << "\"";
    for (size_t i = 0; i < type_signatures.size(); i++) {
        if (type_signatures[i] == "bytes" || type_signatures[i] == "unknown128" || type_signatures[i] == "unknown64") {
            code << ", fmt_hex(arg_" << (i + 1) << ")";
        } else {
            code << ", arg_" << (i + 1);
        }
    }
    code << ");\n";

    code << "}\n";

    return code.str();
}


extern "C" const tracepoint_entry __start_tracepoints;
extern "C" const tracepoint_entry __stop_tracepoints;

std::string generate_epilogue() {
    std::stringstream code;
    code << "constexpr uint64_t start_tracepoints = " << reinterpret_cast<uint64_t>(&__start_tracepoints) << "ULL;\n";
    code << "constexpr uint64_t sizeof_tracepoint_entry = " << sizeof(tracepoint_entry) << "ULL;\n";
    code << R"""(
#include <iostream>
#include <span>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <file>\n";
        return 1;
    }

    int fd = open(argv[1], O_RDONLY);
    if (fd == -1) {
        std::cerr << "Failed to open file: " << argv[1] << "\n";
        return 1;
    }

    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        std::cerr << "Failed to get file size\n";
        close(fd);
        return 1;
    }

    char* mapped_data = static_cast<char*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (mapped_data == MAP_FAILED) {
        std::cerr << "Failed to map file\n";
        close(fd);
        return 1;
    }

    const auto start = reinterpret_cast<const std::byte*>(mapped_data);
    auto p = start;
    const auto end = p + sb.st_size;

    while (p < end) {
        auto addr = read_unaligned<uint64_t>(p);
        auto id = (addr - start_tracepoints) / sizeof_tracepoint_entry;
        (deserializers[id])(p);
    }

    // Cleanup
    munmap(mapped_data, sb.st_size);
    close(fd);
    return 0;
}
)""";
    return code.str();
}

int tracepoint_decoder_main(int argc, char** argv) {
    std::cout << generate_prologue();
    int id = 0;
    size_t max_fileline_size = 0;
    for (const tracepoint_entry* e = &__start_tracepoints; e < &__stop_tracepoints; ++e) {
        max_fileline_size = std::max(max_fileline_size, fmt::format("{}:{}", e->file, e->line).size());
    }
    for (const tracepoint_entry* e = &__start_tracepoints; e < &__stop_tracepoints; ++e) {
        std::cout << generate_deserializer(id++, *e, max_fileline_size);
    }
    std::cout << "void (*deserializers[])(const std::byte*&) = {\n";
    id = 0;
    for (const tracepoint_entry* e = &__start_tracepoints; e < &__stop_tracepoints; ++e) {
        std::cout << "deserialize_" << id++ << ",\n";
    }
    std::cout << "};\n";
    std::cout << generate_epilogue();
    return 0;
}


} // namespace seastar
