#include <doctest.h>

#include "subplane/decoder/sbe_lint.h"

#include <string>

using namespace subplane;

namespace {

std::string schema(int version, const std::string& messages) {
    return R"(<?xml version="1.0"?>
<sbe:messageSchema xmlns:sbe="http://fixprotocol.io/2016/sbe" package="t" id="42" version=")" + std::to_string(version) + R"(" byteOrder="littleEndian">
  <types>
    <composite name="messageHeader"><type name="blockLength" primitiveType="uint16"/><type name="templateId" primitiveType="uint16"/><type name="schemaId" primitiveType="uint16"/><type name="version" primitiveType="uint16"/></composite>
    <composite name="groupSizeEncoding"><type name="blockLength" primitiveType="uint16"/><type name="numInGroup" primitiveType="uint16"/></composite>
    <composite name="varData32"><type name="length" primitiveType="uint32"/><type name="varData" primitiveType="uint8" length="0"/></composite>
    <type name="Sym8" primitiveType="char" length="8"/>
    <type name="Exp9" primitiveType="int8" presence="constant">-9</type>
  </types>
)" + messages + "\n</sbe:messageSchema>";
}

SbeSchema load(const std::string& xml) {
    SbeSchema s;
    std::string err;
    REQUIRE_MESSAGE(s.load_string(xml, err), err);
    return s;
}

int count(const std::vector<LintFinding>& v, LintFinding::Level l, const std::string& needle) {
    int n = 0;
    for (const auto& f : v) if (f.level == l && (f.where + ": " + f.message).find(needle) != std::string::npos) ++n;
    return n;
}

const char* kTrade = R"(
  <sbe:message name="Trade" id="1">
    <field name="InstrumentId" id="1" type="uint32"/>
    <field name="VenueId" id="2" type="uint16"/>
    <field name="Flags" id="3" type="uint16"/>
    <field name="Sequence" id="4" type="uint64"/>
    <field name="Price" id="5" type="int64"/>
  </sbe:message>)";

} // namespace

TEST_CASE("lint data profile: routing fields, offsets, alignment, floats, strings, flatness") {
    const auto s = load(schema(1, std::string(kTrade) + R"(
  <sbe:message name="Quote" id="2">
    <field name="Sequence" id="1" type="uint64"/>
    <field name="InstrumentId" id="2" type="uint32"/>
    <field name="Bid" id="3" type="double"/>
    <field name="Odd" id="4" type="uint64" offset="13"/>
  </sbe:message>
  <sbe:message name="Book" id="3">
    <field name="InstrumentId" id="1" type="uint32"/>
    <field name="Pad" id="2" type="uint32"/>
    <field name="Sequence" id="3" type="uint64"/>
    <group name="Levels" id="4"><field name="Px" id="5" type="int64"/></group>
  </sbe:message>
  <sbe:message name="Named" id="4">
    <field name="Symbol" id="1" type="Sym8"/>
    <field name="Sequence" id="2" type="uint64"/>
  </sbe:message>
  <sbe:message name="Heartbeat" id="100">
    <field name="Ts" id="1" type="double"/>
  </sbe:message>)"));
    LintOptions o;
    o.routing_fields = {"InstrumentId", "Sequence"};
    o.admin_ids = {100};
    const auto f = lint_schema(s, o);
    CHECK(count(f, LintFinding::Level::ERROR, "Named: routing field InstrumentId missing") == 1);
    CHECK(count(f, LintFinding::Level::WARNING, "InstrumentId: routing field at different offsets") == 1);
    CHECK(count(f, LintFinding::Level::WARNING, "Sequence: routing field at different offsets") == 1);
    CHECK(count(f, LintFinding::Level::WARNING, "Quote.Bid: floating point") == 1);
    CHECK(count(f, LintFinding::Level::WARNING, "Quote.Odd: not naturally aligned") == 1);
    CHECK(count(f, LintFinding::Level::WARNING, "Book: groups or varData") == 1);
    CHECK(count(f, LintFinding::Level::WARNING, "Heartbeat") == 0); // admin: exempt from the data rules
}

TEST_CASE("lint control profile: CorrelationId first, reserved template range") {
    const auto s = load(schema(1, R"(
  <sbe:message name="Ping" id="1"><field name="CorrelationId" id="1" type="uint64"/><data name="Text" id="2" type="varData32"/></sbe:message>
  <sbe:message name="Bad" id="2"><field name="Seq" id="1" type="uint32"/><field name="CorrelationId" id="2" type="uint64" offset="8"/></sbe:message>
  <sbe:message name="Odd" id="950"><field name="CorrelationId" id="1" type="uint64"/></sbe:message>)"));
    LintOptions o;
    o.control = true;
    const auto f = lint_schema(s, o);
    CHECK(count(f, LintFinding::Level::ERROR, "Bad: a control message's root must start with CorrelationId") == 1);
    CHECK(count(f, LintFinding::Level::ERROR, "Ping") == 0);
    CHECK(count(f, LintFinding::Level::WARNING, "Odd: template ids 901-999 are reserved") == 1);
    CHECK(count(f, LintFinding::Level::WARNING, "groups or varData") == 0); // fine in control schemas
}

TEST_CASE("lint evolution: appends with sinceVersion pass; removals, moves, retypes and missing bumps fail") {
    const auto base = load(schema(1, kTrade));
    LintOptions o;
    o.baseline = &base;

    const auto good = load(schema(2, R"(
  <sbe:message name="Trade" id="1">
    <field name="InstrumentId" id="1" type="uint32"/>
    <field name="VenueId" id="2" type="uint16"/>
    <field name="Flags" id="3" type="uint16"/>
    <field name="Sequence" id="4" type="uint64"/>
    <field name="Price" id="5" type="int64"/>
    <field name="Qty" id="6" type="int64" sinceVersion="2"/>
  </sbe:message>
  <sbe:message name="Status" id="6"><field name="InstrumentId" id="1" type="uint32"/></sbe:message>)"));
    auto f = lint_schema(good, o);
    CHECK(count(f, LintFinding::Level::ERROR, "") == 0);
    CHECK(count(f, LintFinding::Level::WARNING, "sinceVersion") == 0);

    // A trailing constant (as every canonical message has) is not "moved" when a field goes before it.
    const auto with_const = load(schema(1, R"(
  <sbe:message name="Trade" id="1">
    <field name="InstrumentId" id="1" type="uint32"/>
    <field name="VenueId" id="2" type="uint16"/>
    <field name="Flags" id="3" type="uint16"/>
    <field name="Sequence" id="4" type="uint64"/>
    <field name="Price" id="5" type="int64"/>
    <field name="Exp" id="9" type="Exp9"/>
  </sbe:message>)"));
    const auto appended = load(schema(2, R"(
  <sbe:message name="Trade" id="1">
    <field name="InstrumentId" id="1" type="uint32"/>
    <field name="VenueId" id="2" type="uint16"/>
    <field name="Flags" id="3" type="uint16"/>
    <field name="Sequence" id="4" type="uint64"/>
    <field name="Price" id="5" type="int64"/>
    <field name="Qty" id="6" type="int64" sinceVersion="2"/>
    <field name="Exp" id="9" type="Exp9"/>
  </sbe:message>)"));
    LintOptions oc;
    oc.baseline = &with_const;
    f = lint_schema(appended, oc);
    CHECK(count(f, LintFinding::Level::ERROR, "") == 0);

    const auto bad = load(schema(1, R"(
  <sbe:message name="Trade" id="1">
    <field name="InstrumentId" id="1" type="uint64"/>
    <field name="Sequence" id="4" type="uint64" offset="12"/>
    <field name="Qty" id="6" type="int64"/>
  </sbe:message>)"));
    f = lint_schema(bad, o);
    CHECK(count(f, LintFinding::Level::ERROR, "Trade.VenueId: removed") == 1);
    CHECK(count(f, LintFinding::Level::ERROR, "Trade.Price: removed") == 1);
    CHECK(count(f, LintFinding::Level::ERROR, "Trade.InstrumentId: type changed") == 1);
    CHECK(count(f, LintFinding::Level::ERROR, "Trade.Sequence: moved") == 1);
    CHECK(count(f, LintFinding::Level::ERROR, "Trade.Qty: added inside the old block") == 1);
    CHECK(count(f, LintFinding::Level::ERROR, "Trade.Qty: added without sinceVersion") == 1);
    CHECK(count(f, LintFinding::Level::ERROR, "version did not") == 1);

    const auto other_id = load(R"(<?xml version="1.0"?>
<sbe:messageSchema xmlns:sbe="http://fixprotocol.io/2016/sbe" package="t" id="43" version="2" byteOrder="littleEndian">
  <types><composite name="messageHeader"><type name="blockLength" primitiveType="uint16"/><type name="templateId" primitiveType="uint16"/><type name="schemaId" primitiveType="uint16"/><type name="version" primitiveType="uint16"/></composite></types>
  <sbe:message name="Trade" id="1"><field name="InstrumentId" id="1" type="uint32"/></sbe:message>
</sbe:messageSchema>)");
    f = lint_schema(other_id, o);
    CHECK(count(f, LintFinding::Level::ERROR, "schemaId changed") == 1);
}
