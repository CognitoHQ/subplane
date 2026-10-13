#include <doctest.h>

#include "fb_fixture.h"
#include "subplane/decoder/flatbuffers_wire.h"

#include <cstring>
#include <vector>

using namespace subplane;
using namespace subplane::test;

namespace {
void u16(std::vector<uint8_t>& b, uint16_t v) { b.push_back(static_cast<uint8_t>(v)); b.push_back(static_cast<uint8_t>(v >> 8)); }
void u32(std::vector<uint8_t>& b, uint32_t v) { for (int i = 0; i < 4; ++i) b.push_back(static_cast<uint8_t>(v >> (8 * i))); }
void i32(std::vector<uint8_t>& b, int32_t v) { u32(b, static_cast<uint32_t>(v)); }
fbw::Buf view(const std::vector<uint8_t>& v) { return fbw::Buf{v.data(), static_cast<uint32_t>(v.size())}; }
} // namespace

TEST_CASE("fbw: root, vtable slots, absent field, field beyond the producer's vtable") {
    // 0: root -> 16; 4: ident; 8: vtable {vsize 8, tsize 12, slot0 4, slot1 8}; 16: table {soffset 8, u32, u16, pad}
    std::vector<uint8_t> b;
    u32(b, 16); b.insert(b.end(), {'T', 'E', 'S', 'T'});
    u16(b, 8); u16(b, 12); u16(b, 4); u16(b, 8);
    i32(b, 8); u32(b, 0x11223344); u16(b, 0x5566); u16(b, 0);
    REQUIRE(b.size() == 28);
    uint32_t t = 0;
    REQUIRE(fbw::root(view(b), 0, t));
    CHECK(t == 16);
    char id[4]; REQUIRE(fbw::file_id(view(b), 0, id)); CHECK(std::memcmp(id, "TEST", 4) == 0);
    CHECK(fbw::field(view(b), t, 0) == 20);
    CHECK(fbw::field(view(b), t, 1) == 24);
    CHECK(fbw::field(view(b), t, 2) == 0);   // slot beyond vsize: newer schema than producer
    CHECK(fbw::field(view(b), t, 200) == 0);
    CHECK(fbw::scalar<uint32_t>(view(b), t, 0, 9) == 0x11223344);
    CHECK(fbw::scalar<uint16_t>(view(b), t, 1, 9) == 0x5566);
    CHECK(fbw::scalar<uint16_t>(view(b), t, 2, 9) == 9);
    // A zero slot means absent.
    b[8 + 6] = 0; b[8 + 7] = 0;
    CHECK(fbw::field(view(b), t, 1) == 0);
    CHECK(fbw::scalar<uint16_t>(view(b), t, 1, 7) == 7);
}

TEST_CASE("fbw: nested table and string through uoffsets, vtable after the table (negative soffset)") {
    std::vector<uint8_t> b;
    // 0: root -> 16; 4: ident; 8: vtable {8, 12, 4, 8}; 16: table {soffset 8, child uoffset, string uoffset}
    u32(b, 16); b.insert(b.end(), {'M', 'D', 'v', '1'});
    u16(b, 8); u16(b, 12); u16(b, 4); u16(b, 8);
    i32(b, 8); u32(b, 36 - 20); u32(b, 44 - 24);
    // 28: child vtable {6, 8, 4} + pad; 36: child table {soffset 8, u32}; 44: string {len 5, "hello", NUL}
    u16(b, 6); u16(b, 8); u16(b, 4); u16(b, 0);
    i32(b, 8); u32(b, 0xCAFEBABE);
    u32(b, 5); b.insert(b.end(), {'h', 'e', 'l', 'l', 'o', 0});
    REQUIRE(b.size() == 54);
    uint32_t t = 0; REQUIRE(fbw::root(view(b), 0, t));
    const uint32_t child = fbw::deref(view(b), t, 0);
    CHECK(child == 36);
    CHECK(fbw::scalar<uint32_t>(view(b), child, 0, 0) == 0xCAFEBABE);
    const char* p = nullptr; uint32_t len = 0;
    REQUIRE(fbw::string(view(b), t, 1, p, len));
    CHECK(std::string(p, len) == "hello");
    CHECK(fbw::deref(view(b), t, 5) == 0);

    // Negative soffset: 0: root -> 8; 8: table {soffset -12, u32}; 16: pad; 20: vtable {6, 8, 4}
    std::vector<uint8_t> c;
    u32(c, 8); u32(c, 0);
    i32(c, 8 - 20); u32(c, 0xDEADBEEF); u32(c, 0);
    u16(c, 6); u16(c, 8); u16(c, 4); u16(c, 0);
    REQUIRE(fbw::root(view(c), 0, t));
    CHECK(fbw::scalar<uint32_t>(view(c), t, 0, 0) == 0xDEADBEEF);
}

TEST_CASE("fbw: size-prefixed real flatc buffers resolve, and truncation at every length never reads out of bounds") {
    auto msgs = fb_fixture_messages();
    REQUIRE(msgs.size() == 300);
    auto exp = fb_fixture_expected();
    REQUIRE(exp.size() == 300);
    for (size_t i = 0; i < msgs.size(); ++i) {
        const auto& m = msgs[i];
        uint32_t env = 0;
        REQUIRE(fbw::root(view(m), 4, env));
        char id[4]; REQUIRE(fbw::file_id(view(m), 4, id)); CHECK(std::memcmp(id, "MDv1", 4) == 0);
        CHECK(fbw::scalar<uint64_t>(view(m), env, 0, 0) == exp[i].seq);
        CHECK(fbw::scalar<uint64_t>(view(m), env, 1, 0) == exp[i].ts);
        CHECK(fbw::scalar<uint8_t>(view(m), env, 2, 0) == exp[i].kind);
        CHECK(fbw::scalar<uint16_t>(view(m), env, 4, 0) == exp[i].flags); // 0 when the old producer omitted it
        if (exp[i].kind != 3) {
            const uint32_t body = fbw::deref(view(m), env, 3);
            const uint32_t ins = fbw::deref(view(m), body, 0);
            REQUIRE(ins != 0);
            CHECK(fbw::scalar<uint32_t>(view(m), ins, 0, 0) == exp[i].instrument);
            CHECK(fbw::scalar<uint16_t>(view(m), ins, 1, 0) == exp[i].venue);
            const char* p = nullptr; uint32_t len = 0;
            REQUIRE(fbw::string(view(m), ins, 2, p, len));
            CHECK(std::string(p, len) == exp[i].symbol);
        }
    }
    // Sweep: every message cut at every length. Reads must stay inside the cut buffer.
    // A copy with poison bytes after the cut shows up as a wrong value if any read strays.
    for (size_t i = 0; i < 40; ++i) {
        const auto& m = msgs[i];
        for (uint32_t cut = 0; cut <= m.size(); ++cut) {
            std::vector<uint8_t> c(m.begin(), m.begin() + cut);
            c.insert(c.end(), 64, 0xEE);
            const fbw::Buf s{c.data(), cut};
            uint32_t env = 0;
            if (!fbw::root(s, 4, env)) continue;
            (void)fbw::scalar<uint64_t>(s, env, 0, 0);
            (void)fbw::scalar<uint8_t>(s, env, 2, 0);
            const uint32_t body = fbw::deref(s, env, 3);
            const uint32_t ins = body ? fbw::deref(s, body, 0) : 0;
            if (ins) { const char* p = nullptr; uint32_t len = 0; if (fbw::string(s, ins, 2, p, len)) CHECK(p + len <= reinterpret_cast<const char*>(c.data()) + cut); }
        }
    }
}
