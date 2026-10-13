#pragma once
// Loads tests/fixtures/flatbuffers/* (real flatc output; see its README.md).
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

namespace subplane::test {

inline std::string fb_fixture_path(const char* name) {
    std::string f = __FILE__;
    return f.substr(0, f.find_last_of('/')) + "/../fixtures/flatbuffers/" + name;
}

// Each element is one size-prefixed buffer (u32 size + buffer), as sent on the wire.
inline std::vector<std::vector<uint8_t>> fb_fixture_messages() {
    std::ifstream f(fb_fixture_path("fb_sample.bin"), std::ios::binary);
    std::vector<uint8_t> all((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    std::vector<std::vector<uint8_t>> out;
    size_t p = 0;
    while (p + 4 <= all.size()) {
        uint32_t n; std::memcpy(&n, all.data() + p, 4);
        if (p + 4 + n > all.size()) break;
        out.emplace_back(all.begin() + static_cast<long>(p), all.begin() + static_cast<long>(p + 4 + n));
        p += 4 + n;
    }
    return out;
}

struct FbExpected { int kind; uint64_t seq, ts; uint32_t instrument; uint16_t venue; std::string symbol; uint16_t flags; };

inline std::vector<FbExpected> fb_fixture_expected() {
    std::vector<FbExpected> out;
    std::ifstream f(fb_fixture_path("fb_sample.expected"));
    std::string line;
    while (std::getline(f, line)) {
        FbExpected e; char sym[32]; unsigned long long seq, ts; unsigned iid, venue, flags;
        if (std::sscanf(line.c_str(), "%d %llu %llu %u %u %31s %u", &e.kind, &seq, &ts, &iid, &venue, sym, &flags) != 7) continue;
        e.seq = seq; e.ts = ts; e.instrument = iid; e.venue = static_cast<uint16_t>(venue); e.symbol = sym; e.flags = static_cast<uint16_t>(flags);
        out.push_back(e);
    }
    return out;
}

} // namespace subplane::test
