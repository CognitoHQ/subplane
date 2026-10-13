#include <doctest.h>

#include <string>
#include <vector>

#include "subplane/framing/sbe_length.h"

using namespace subplane;

namespace {
std::string sample_path() {
    std::string f = __FILE__;
    return f.substr(0, f.find_last_of('/')) + "/../data/sample_md.xml";
}

struct Buf {
    std::vector<uint8_t> b;
    void u(uint64_t v, unsigned size) {
        for (unsigned i = 0; i < size; ++i) b.push_back(static_cast<uint8_t>(v >> (8 * i)));
    }
    void zeros(size_t n) { b.insert(b.end(), n, 0); }
    void header(uint16_t blk, uint16_t tid, uint16_t version = 2) { u(blk, 2); u(tid, 2); u(7, 2); u(version, 2); }
};

// Trade: 8 + 39
Buf trade() { Buf x; x.header(39, 1); x.zeros(39); return x; }

// BookUpdate with levels[i] = number of orders in level i, and a note of `note` bytes.
Buf book(const std::vector<unsigned>& orders_per_level, unsigned note, uint16_t blk = 26) {
    Buf x;
    x.header(blk, 3);
    x.zeros(blk);
    x.u(13, 2); x.u(orders_per_level.size(), 2);
    for (unsigned o : orders_per_level) {
        x.zeros(13);
        x.u(12, 2); x.u(o, 2);
        x.zeros(12u * o);
    }
    x.u(note, 2);
    x.zeros(note);
    return x;
}

struct Fixture {
    SbeSchema schema;
    SbeLengthWalker w;
    Fixture() {
        std::string err;
        REQUIRE_MESSAGE(schema.load_file(sample_path(), err), err);
        REQUIRE_MESSAGE(w.build(schema, err), err);
    }
    WalkStatus walk(const Buf& x, uint32_t avail, uint32_t& len) const { return w.walk(x.b.data(), avail, len); }
};
} // namespace

TEST_CASE("walker: header size and programs") {
    Fixture f;
    CHECK(f.w.header_size() == 8);
    REQUIRE(f.w.program(1));
    CHECK(f.w.program(1)->count == 0);
    REQUIRE(f.w.program(3));
    CHECK(f.w.program(3)->count == 5); // Levels{ Orders{}, } + Note ... begin,begin,end,end,data
    CHECK(f.w.program(5) == nullptr);
    CHECK(f.w.program(60000) == nullptr);
}

TEST_CASE("walker: fixed message") {
    Fixture f;
    Buf t = trade();
    uint32_t len = 0;
    CHECK(f.walk(t, static_cast<uint32_t>(t.b.size()), len) == WalkStatus::OK);
    CHECK(len == 47);
    // trailing bytes (next message) are not consumed
    t.zeros(20);
    CHECK(f.walk(t, static_cast<uint32_t>(t.b.size()), len) == WalkStatus::OK);
    CHECK(len == 47);
}

TEST_CASE("walker: groups and data lengths") {
    Fixture f;
    uint32_t len = 0;
    struct C { std::vector<unsigned> lv; unsigned note; };
    const C cases[] = {{{}, 0}, {{}, 5}, {{0}, 0}, {{2}, 5}, {{1, 0, 3}, 0}, {{1, 0, 3}, 5}};
    for (const auto& c : cases) {
        Buf x = book(c.lv, c.note);
        uint32_t expect = 8 + 26 + 4 + 2 + c.note;
        for (unsigned o : c.lv) expect += 13 + 4 + 12 * o;
        REQUIRE(expect == x.b.size());
        CHECK(f.walk(x, static_cast<uint32_t>(x.b.size()), len) == WalkStatus::OK);
        CHECK(len == expect);
    }
}

TEST_CASE("walker: truncation at every stage") {
    Fixture f;
    Buf x = book({1, 0, 3}, 5);
    uint32_t len = 99;
    // every strict prefix must be non-OK and report 0 length; prefixes shorter than the full message
    for (uint32_t n = 0; n < x.b.size(); ++n) {
        WalkStatus s = f.walk(x, n, len);
        CHECK_MESSAGE(s != WalkStatus::OK, "prefix " << n);
        CHECK(len == 0);
        CHECK_MESSAGE((s == WalkStatus::TRUNCATED || s == WalkStatus::MALFORMED), "prefix " << n);
    }
    CHECK(f.walk(x, 7, len) == WalkStatus::TRUNCATED);        // header
    CHECK(f.walk(x, 8 + 25, len) == WalkStatus::TRUNCATED);   // root block
    CHECK(f.walk(x, 8 + 26 + 3, len) == WalkStatus::TRUNCATED); // dimension header
    // data length prefix / body
    Buf d = book({}, 5);
    CHECK(f.walk(d, static_cast<uint32_t>(d.b.size()) - 6 + 1, len) == WalkStatus::TRUNCATED); // 1 byte of prefix
    CHECK(f.walk(d, static_cast<uint32_t>(d.b.size()) - 1, len) == WalkStatus::TRUNCATED);     // body short
}

TEST_CASE("walker: malformed counts") {
    Fixture f;
    Buf x = book({}, 0);
    // patch numInGroup (offset 8+26+2) to a huge value
    x.b[8 + 26 + 2] = 0xFF; x.b[8 + 26 + 3] = 0xFF;
    uint32_t len = 0;
    CHECK(f.walk(x, static_cast<uint32_t>(x.b.size()), len) == WalkStatus::MALFORMED);
    CHECK(len == 0);
}

TEST_CASE("walker: larger blockLength than schema (forward compat)") {
    Fixture f;
    Buf x = book({2}, 3, 30); // publisher added 4 root bytes
    uint32_t len = 0;
    CHECK(f.walk(x, static_cast<uint32_t>(x.b.size()), len) == WalkStatus::OK);
    CHECK(len == x.b.size());
    Buf t; t.header(45, 1); t.zeros(45);
    CHECK(f.walk(t, static_cast<uint32_t>(t.b.size()), len) == WalkStatus::OK);
    CHECK(len == 53);
}

TEST_CASE("walker: unknown template") {
    Fixture f;
    Buf x; x.header(4, 55); x.zeros(4);
    uint32_t len = 5;
    CHECK(f.walk(x, static_cast<uint32_t>(x.b.size()), len) == WalkStatus::UNKNOWN_TEMPLATE);
    CHECK(len == 0);
}

TEST_CASE("walker: big endian") {
    SbeSchema s;
    std::string err;
    std::string xml;
    {
        // reuse sample with byte order swapped
        SbeSchema tmp;
        REQUIRE(tmp.load_file(sample_path(), err));
    }
    FILE* fp = std::fopen(sample_path().c_str(), "rb");
    REQUIRE(fp);
    char c;
    while (std::fread(&c, 1, 1, fp) == 1) xml.push_back(c);
    std::fclose(fp);
    xml.replace(xml.find("littleEndian"), 12, "bigEndian");
    REQUIRE_MESSAGE(s.load_string(xml, err), err);
    SbeLengthWalker w;
    REQUIRE(w.build(s, err));
    std::vector<uint8_t> m = {0, 39, 0, 1, 0, 7, 0, 2};
    m.resize(47, 0);
    uint32_t len = 0;
    CHECK(w.walk(m.data(), 47, len) == WalkStatus::OK);
    CHECK(len == 47);
}

// Regression: jump indices must be program-relative. The grouped template is
// deliberately not the first grouped message in the schema, and the groups have
// more than two entries; both conditions are needed to expose the bug. Layout
// mirrors CME MDP 3.0 (3-byte groupSize, 8-byte aligned groupSize8Byte).
TEST_CASE("walker: multi-entry groups in a non-first grouped template (CME shape)") {
    static const char* xml = R"(<?xml version="1.0"?>
<sbe:messageSchema xmlns:sbe="http://fixprotocol.io/2016/sbe" package="t" id="1" version="9" byteOrder="littleEndian">
  <types>
    <composite name="messageHeader"><type name="blockLength" primitiveType="uint16"/><type name="templateId" primitiveType="uint16"/><type name="schemaId" primitiveType="uint16"/><type name="version" primitiveType="uint16"/></composite>
    <composite name="groupSize"><type name="blockLength" primitiveType="uint16"/><type name="numInGroup" primitiveType="uint8"/></composite>
    <composite name="groupSize8Byte"><type name="blockLength" primitiveType="uint16"/><type name="numInGroup" offset="7" primitiveType="uint8"/></composite>
  </types>
  <sbe:message name="First" id="1" blockLength="4">
    <field name="a" id="1" type="uint32" offset="0"/>
    <group name="G" id="10" blockLength="8" dimensionType="groupSize"><field name="b" id="2" type="uint64" offset="0"/></group>
  </sbe:message>
  <sbe:message name="Inc" id="48" blockLength="11">
    <field name="TransactTime" id="60" type="uint64" offset="0"/>
    <field name="Mei" id="5799" type="uint8" offset="8"/>
    <group name="NoMDEntries" id="268" blockLength="32" dimensionType="groupSize"><field name="px" id="270" type="int64" offset="0"/></group>
    <group name="NoOrderIDEntries" id="37705" blockLength="16" sinceVersion="7" dimensionType="groupSize8Byte"><field name="oid" id="37" type="uint64" offset="0"/></group>
  </sbe:message>
</sbe:messageSchema>)";
    SbeSchema schema;
    SbeLengthWalker w;
    std::string err;
    REQUIRE_MESSAGE(schema.load_string(xml, err), err);
    REQUIRE_MESSAGE(w.build(schema, err), err);
    const unsigned counts[][2] = {{0, 0}, {1, 0}, {4, 0}, {4, 3}, {1, 3}, {7, 1}};
    for (const auto& c : counts) {
        Buf x;
        x.header(11, 48, 9); x.zeros(11);
        x.u(32, 2); x.u(c[0], 1); x.zeros(32u * c[0]);
        x.u(16, 2); x.zeros(5); x.u(c[1], 1); x.zeros(16u * c[1]);
        uint32_t len = 0;
        CHECK(w.walk(x.b.data(), static_cast<uint32_t>(x.b.size()), len) == WalkStatus::OK);
        CHECK(len == x.b.size());
        // Also with more bytes available than the message: the walk must stop at the message end.
        x.zeros(64);
        CHECK(w.walk(x.b.data(), static_cast<uint32_t>(x.b.size()), len) == WalkStatus::OK);
        CHECK(len == x.b.size() - 64);
    }
    // Acting version 6 < sinceVersion 7: the second group is not on the wire.
    // Whatever follows the first group (here: another message) must not be read as its header.
    Buf old;
    old.header(11, 48, 6); old.zeros(11);
    old.u(32, 2); old.u(4, 1); old.zeros(32u * 4);
    const size_t msg_end = old.b.size();
    old.u(16, 2); old.zeros(5); old.u(3, 1); old.zeros(48);  // bytes of a following message
    uint32_t len = 0;
    CHECK(w.walk(old.b.data(), static_cast<uint32_t>(old.b.size()), len) == WalkStatus::OK);
    CHECK(len == msg_end);
    CHECK(w.walk(old.b.data(), static_cast<uint32_t>(msg_end), len) == WalkStatus::OK);
    CHECK(len == msg_end);
}
