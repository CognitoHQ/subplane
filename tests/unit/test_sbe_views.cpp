#include <doctest.h>

#include "subplane/decoder/sbe_views.h"

#include <string>

using namespace subplane;

namespace {
std::string sample_path() { std::string f = __FILE__; return f.substr(0, f.find_last_of('/')) + "/../data/sample_md.xml"; }
SbeSchema load_sample() { SbeSchema s; std::string err; REQUIRE_MESSAGE(s.load_file(sample_path(), err), err); return s; }
bool has(const std::string& hay, const std::string& needle) { return hay.find(needle) != std::string::npos; }
}

TEST_CASE("views: flat layouts of the sample schema, alignment verdicts, skipped messages") {
    SbeSchema s = load_sample();
    CHECK(s.package() == "sample.md");
    CHECK(views_basename(s) == "sample_md");
    FlatLayout l; std::string reason;
    // Trade: Price i64 at root 26 -> payload 34: misaligned.
    REQUIRE(flat_layout(s, *s.find_message(1), l, reason));
    CHECK(l.size == 8 + 39);
    CHECK_FALSE(l.aligned);
    CHECK(l.misaligned_field == "Price at offset 34");
    CHECK(l.slots.size() == 4 + 7);   // header members + fields, no padding (block ends at Side)
    CHECK(l.slots[4].name == "SecurityID"); CHECK(l.slots[4].offset == 8);
    CHECK(l.slots[9].name == "Qty"); CHECK(l.slots[9].type_name == "Qty32");
    CHECK(l.slots[10].name == "Side"); CHECK(l.slots[10].type_name == "SideEnum");
    // Quote: BidPx at explicit root 32 -> gap of 6 after MarketSegmentID; AskPx lands at payload 52: misaligned.
    REQUIRE(flat_layout(s, *s.find_message(2), l, reason));
    CHECK_FALSE(l.aligned);
    CHECK(l.misaligned_field == "AskPx at offset 52");
    CHECK(l.size == 8 + 56);
    bool pad = false;
    for (const auto& v : l.slots) if (v.kind == ViewSlot::Kind::PAD) { pad = true; CHECK(v.offset == 34); CHECK(v.size == 6); }
    CHECK(pad);
    // Ticker: char[8] then u64s: aligned (char arrays are 1-aligned).
    REQUIRE(flat_layout(s, *s.find_message(4), l, reason));
    CHECK(l.aligned);
    CHECK(l.slots[4].prim == SbePrim::CHAR_ARRAY); CHECK(l.slots[4].size == 8);
    // BookUpdate has groups.
    CHECK_FALSE(flat_layout(s, *s.find_message(3), l, reason));
    CHECK(has(reason, "groups (Levels)"));
}

TEST_CASE("views: C++ emitter text") {
    SbeSchema s = load_sample();
    ViewsOptions o; ViewsReport rep; std::string out, err;
    REQUIRE_MESSAGE(emit_cpp(s, o, out, rep, err), err);
    CHECK(rep.emitted == std::vector<std::string>{"Trade", "Quote", "Ticker", "Heartbeat", "SeqReset"});
    REQUIRE(rep.skipped.size() == 1);
    CHECK(has(rep.skipped[0], "BookUpdate: has groups"));
    CHECK(has(out, "namespace sp_views {"));
    CHECK(has(out, "class Trade {"));
    CHECK(has(out, "Price at offset 34 is not naturally aligned"));
    CHECK(has(out, "int64_t Price() const noexcept { return detail::load<int64_t>(p_, 34); }"));
    CHECK(has(out, "class Quote {"));
    CHECK(has(out, "AskPx at offset 52 is not naturally aligned"));
    CHECK(has(out, "int64_t BidPx() const noexcept { return detail::load<int64_t>(p_, 40); }"));
    CHECK(has(out, "struct Ticker {"));
    CHECK(has(out, "static_assert(offsetof(Ticker, SeqNum) == 16);"));
    CHECK(has(out, "static_assert(sizeof(Ticker) == Ticker::kSize);"));
    CHECK(has(out, "char Symbol[8];"));
    CHECK(has(out, "uint8_t Side() const noexcept { return detail::load<uint8_t>(p_, 46); }  // SideEnum"));
    CHECK_FALSE(has(out, "#pragma pack("));
    // Filter and namespace.
    o.ns = "md"; o.only = {"Heartbeat"}; rep = {};
    REQUIRE(emit_cpp(s, o, out, rep, err));
    CHECK(rep.emitted == std::vector<std::string>{"Heartbeat"});
    CHECK(has(out, "namespace md {"));
    CHECK_FALSE(has(out, "class Quote"));
    o.only = {"Nope"};
    CHECK_FALSE(emit_cpp(s, o, out, rep, err));
    CHECK(has(err, "no message named 'Nope'"));
}

TEST_CASE("views: NumPy and q emitters") {
    SbeSchema s = load_sample();
    ViewsOptions o; ViewsReport rep; std::string out, err;
    REQUIRE_MESSAGE(emit_numpy(s, o, out, rep, err), err);
    CHECK(has(out, "    import numpy as np"));
    CHECK(has(out, "class RecordHeader:"));
    CHECK(has(out, "FORMAT = '<QQQQIIHHHHHBBBB10x'"));
    CHECK(has(out, "RECORD_HEADER = np.dtype("));
    CHECK(has(out, "'itemsize': 64"));
    CHECK(has(out, "class Trade:"));
    CHECK(has(out, "    Side: int  # SideEnum"));
    CHECK(has(out, "FORMAT = '<HHHHQQQHqIB'"));
    CHECK(has(out, "FORMAT = '<HHHHQQQH6xqIqI'"));   // Quote: the 6-byte gap
    CHECK(has(out, "FORMAT = '<HHHH8sQQ'"));          // Ticker: char[8]
    CHECK(has(out, "Trade.DTYPE = np.dtype({'names': ['blockLength', 'templateId', 'schemaId', 'version', 'SecurityID', 'SeqNum', 'TransactTime', 'MarketSegmentID', 'Price', 'Qty', 'Side'],"));
    CHECK(has(out, "def parse(record, offset=0):"));
    CHECK(has(out, "'formats': ['<u2', '<u2', '<u2', '<u2', '<u8', '<u8', '<u8', '<u2', '<i8', '<u4', 'u1'],"));
    CHECK(has(out, "'offsets': [0, 2, 4, 6, 8, 16, 24, 32, 34, 42, 46],"));
    CHECK(has(out, "'itemsize': 47})"));
    CHECK(has(out, "'S8'"));
    CHECK(has(out, "TEMPLATE_ID = {'Trade': 1, 'Quote': 2, 'Ticker': 4, 'Heartbeat': 100, 'SeqReset': 101}"));
    REQUIRE_MESSAGE(emit_q(s, o, out, rep, err), err);
    CHECK(has(out, ".ft.sample_md.Trade.spec:(\"hhhhjjjhjix\";2 2 2 2 8 8 8 2 8 4 1)"));
    CHECK(has(out, ".ft.sample_md.Quote.spec:(\"hhhhjjjh jiji\";2 2 2 2 8 8 8 2 6 8 4 8 4)"));
    CHECK(has(out, ".ft.sample_md.Ticker.spec:(\"hhhh jj\";2 2 2 2 8 8 8)"));
    CHECK(has(out, "d[`Symbol]:`$\"c\"$b[8+til 8];"));
    CHECK(has(out, ".ft.sample_md.Trade.names:`blockLength`templateId`schemaId`version`SecurityID`SeqNum`TransactTime`MarketSegmentID`Price`Qty`Side"));
}

TEST_CASE("views: big-endian refused, constants omitted and noted, overlap rejected") {
    const char* kBe = R"(<?xml version="1.0"?>
<sbe:messageSchema xmlns:sbe="http://fixprotocol.io/2016/sbe" package="t" id="9" version="1" byteOrder="bigEndian" headerType="messageHeader">
  <types><composite name="messageHeader"><type name="blockLength" primitiveType="uint16"/><type name="templateId" primitiveType="uint16"/><type name="schemaId" primitiveType="uint16"/><type name="version" primitiveType="uint16"/></composite></types>
  <sbe:message name="M" id="1"><field name="A" id="1" type="uint64"/></sbe:message>
</sbe:messageSchema>)";
    SbeSchema be; std::string err;
    REQUIRE_MESSAGE(be.load_string(kBe, err), err);
    ViewsOptions o; ViewsReport rep; std::string out;
    CHECK_FALSE(emit_cpp(be, o, out, rep, err));
    CHECK(has(err, "big-endian"));

    const char* kConst = R"(<?xml version="1.0"?>
<sbe:messageSchema xmlns:sbe="http://fixprotocol.io/2016/sbe" package="t.c" id="9" version="1" byteOrder="littleEndian" headerType="messageHeader">
  <types><composite name="messageHeader"><type name="blockLength" primitiveType="uint16"/><type name="templateId" primitiveType="uint16"/><type name="schemaId" primitiveType="uint16"/><type name="version" primitiveType="uint16"/></composite>
    <type name="Kind" primitiveType="uint8" presence="constant">7</type></types>
  <sbe:message name="M" id="1"><field name="A" id="1" type="uint64"/><field name="K" id="2" type="Kind"/><field name="B" id="3" type="uint32"/></sbe:message>
  <sbe:message name="O" id="2"><field name="A" id="1" type="uint64"/><field name="B" id="2" type="uint32" offset="4"/></sbe:message>
</sbe:messageSchema>)";
    SbeSchema c;
    REQUIRE_MESSAGE(c.load_string(kConst, err), err);
    CHECK(views_basename(c) == "t_c");
    rep = {};
    REQUIRE_MESSAGE(emit_cpp(c, o, out, rep, err), err);
    CHECK(rep.emitted == std::vector<std::string>{"M"});
    CHECK(has(out, "Constant fields omitted: K (Kind)"));
    CHECK(has(out, "static_assert(offsetof(M, B) == 16);"));
    CHECK_FALSE(has(out, " K;"));
    REQUIRE(rep.skipped.size() == 1);
    CHECK(has(rep.skipped[0], "O: field 'B' overlaps"));
}

TEST_CASE("views: scalar arrays are typed in every view; a misaligned array flips to the view form") {
    const char* kArr = R"(<?xml version="1.0"?>
<sbe:messageSchema xmlns:sbe="http://fixprotocol.io/2016/sbe" package="t.a" id="9" version="1" byteOrder="littleEndian" headerType="messageHeader">
  <types><composite name="messageHeader"><type name="blockLength" primitiveType="uint16"/><type name="templateId" primitiveType="uint16"/><type name="schemaId" primitiveType="uint16"/><type name="version" primitiveType="uint16"/></composite>
    <type name="I4" primitiveType="int64" length="4"/><type name="U2" primitiveType="uint64" length="2"/></types>
  <sbe:message name="A" id="1"><field name="X" id="1" type="I4"/><field name="N" id="2" type="uint32"/></sbe:message>
  <sbe:message name="B" id="2"><field name="N" id="1" type="uint32"/><field name="Y" id="2" type="U2"/></sbe:message>
</sbe:messageSchema>)";
    SbeSchema s; std::string err;
    REQUIRE_MESSAGE(s.load_string(kArr, err), err);
    CHECK(s.find_field(*s.find_message(1), "X")->elem_prim == SbePrim::I64);
    ViewsOptions o; ViewsReport rep; std::string out;
    REQUIRE_MESSAGE(emit_cpp(s, o, out, rep, err), err);
    CHECK(has(out, "struct A {"));
    CHECK(has(out, "    int64_t X[4];"));
    CHECK(has(out, "static_assert(offsetof(A, N) == 40);"));
    CHECK(has(out, "class B {"));                       // Y at payload 12: uint64 elements need 8
    CHECK(has(out, "Y at offset 12 is not naturally aligned"));
    CHECK(has(out, "static constexpr size_t kYLen = 2;"));
    CHECK(has(out, "uint64_t Y(size_t i) const noexcept { return detail::load<uint64_t>(p_, 12 + i * 8); }"));
    REQUIRE_MESSAGE(emit_numpy(s, o, out, rep, err), err);
    CHECK(has(out, "    X: tuple"));
    CHECK(has(out, "FORMAT = '<HHHH4qI'"));
    CHECK(has(out, "ARRAYS = [('X', 4)]"));
    CHECK(has(out, "'formats': ['<u2', '<u2', '<u2', '<u2', ('<i8', (4,)), '<u4'],"));
    REQUIRE_MESSAGE(emit_q(s, o, out, rep, err), err);
    CHECK(has(out, ".ft.t_a.A.spec:(\"hhhh i\";2 2 2 2 32 4)"));
    CHECK(has(out, "d[`X]:first (enlist \"j\";enlist 8) 1: b[8+til 32];"));
}
