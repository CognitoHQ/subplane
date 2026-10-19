#include <doctest.h>

#include "subplane/decoder/sbe_codec.h"
#include "subplane/decoder/sbe_schema.h"
#include "subplane/proto/subplane_proto_codec.h"

#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

using namespace subplane;

namespace {

std::string repo_file(const std::string& rel) {
    std::string f = __FILE__;
    return f.substr(0, f.find_last_of('/')) + "/../../" + rel;
}

std::string slurp(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

} // namespace

TEST_CASE("spp codec: the checked-in header is what the generator emits from the schema") {
    SbeSchema s;
    std::string err;
    REQUIRE_MESSAGE(s.load_file(repo_file("schemas/subplane_proto.xml"), err), err);
    CodecOptions o;
    o.ns = "sp_proto";
    std::string out;
    REQUIRE_MESSAGE(emit_cpp_codec(s, o, out, err), err);
    // Regenerate with: subplane schema --schema schemas/subplane_proto.xml --codec cpp --namespace sp_proto --out include/subplane/proto
    CHECK_MESSAGE(out == slurp(repo_file("include/subplane/proto/subplane_proto_codec.h")),
                  "include/subplane/proto/subplane_proto_codec.h is stale; regenerate it");
}

TEST_CASE("spp codec: Hello round trip with its extension group") {
    uint8_t buf[256];
    sp_proto::HelloEncoder e(buf, sizeof(buf));
    e.CorrelationId(7).PeerEpoch(123456789).ProtoVersion(1).MinProtoVersion(1).Role(sp_proto::Role::Gateway).ServiceName("spwsgw");
    e.begin_Extensions(2);
    e.Extensions(0).ExtensionId(0x8001).Version(3);
    e.Extensions(1).ExtensionId(0x0002).Version(1);
    const size_t n = e.finish();
    REQUIRE(n == 8 + 56 + 4 + 2 * 4);

    sp_proto::HelloDecoder d;
    REQUIRE(d.wrap(buf, n));
    CHECK(d.size() == n);
    CHECK(d.CorrelationId() == 7);
    CHECK(d.PeerEpoch() == 123456789);
    CHECK(d.Role() == sp_proto::Role::Gateway);
    CHECK(d.ServiceName() == "spwsgw");
    REQUIRE(d.Extensions_count() == 2);
    CHECK(d.Extensions(0).ExtensionId() == 0x8001);
    CHECK(d.Extensions(0).Version() == 3);
    CHECK(d.Extensions(1).ExtensionId() == 2);
    // Any shorter buffer is refused.
    for (size_t cut = 0; cut < n; ++cut) CHECK_FALSE(d.wrap(buf, cut));
    // Another template is refused.
    sp_proto::StatusDecoder sd;
    CHECK_FALSE(sd.wrap(buf, n));
}

TEST_CASE("spp codec: groups and data not written go out empty; out-of-order and overflow fail") {
    uint8_t buf[256];
    {
        sp_proto::HelloEncoder e(buf, sizeof(buf));
        e.CorrelationId(1);
        const size_t n = e.finish(); // no Extensions written: an empty group
        REQUIRE(n == 8 + 56 + 4);
        sp_proto::HelloDecoder d;
        REQUIRE(d.wrap(buf, n));
        CHECK(d.Extensions_count() == 0);
    }
    {
        sp_proto::StatusEncoder e(buf, sizeof(buf));
        e.CorrelationId(9).Code(sp_proto::StatusCode::Unsupported).Reason(42).Text("no such template");
        const size_t n = e.finish();
        sp_proto::StatusDecoder d;
        REQUIRE(d.wrap(buf, n));
        CHECK(d.Code() == sp_proto::StatusCode::Unsupported);
        CHECK(d.Reason() == 42);
        CHECK(d.Text() == "no such template");
    }
    {
        sp_proto::StatusEncoder e(buf, sizeof(buf));
        e.Text("a");
        e.Text("b"); // data written twice: out of order
        CHECK(e.finish() == 0);
    }
    {
        sp_proto::RecapRecordEncoder e(buf, 40);
        const std::string big(64, 'x');
        e.CorrelationId(3).Payload(big);
        CHECK(e.finish() == 0);
    }
    {
        sp_proto::HelloEncoder e(buf, sizeof(buf));
        e.ServiceName(std::string(33, 'n'));
        CHECK(e.finish() == 0);
    }
    {
        sp_proto::HelloEncoder e(buf, 10); // smaller than header + root
        CHECK_FALSE(e.ok());
        CHECK(e.finish() == 0);
    }
}

TEST_CASE("spp codec: recap reply messages, and a record payload carried byte for byte") {
    uint8_t buf[512];
    size_t at = 0;
    sp_proto::RecapBeginEncoder b(buf + at, sizeof(buf) - at);
    b.CorrelationId(55).AsOf(1000).SourceEpoch(77).RecordCount(1);
    at += b.finish();
    const uint8_t payload[] = {1, 2, 3, 0, 255, 9};
    sp_proto::RecapRecordEncoder r(buf + at, sizeof(buf) - at);
    r.CorrelationId(55).Payload(payload, sizeof(payload));
    const size_t rlen = r.finish();
    REQUIRE(rlen == 8 + 8 + 4 + sizeof(payload));
    sp_proto::RecapRecordDecoder rd;
    REQUIRE(rd.wrap(buf + at, rlen));
    REQUIRE(rd.Payload_size() == sizeof(payload));
    CHECK(std::memcmp(rd.Payload_data(), payload, sizeof(payload)) == 0);
    at += rlen;

    sp_proto::RecapBeginDecoder bd;
    REQUIRE(bd.wrap(buf, at));
    CHECK(bd.AsOf() == 1000);
    CHECK(bd.SourceEpoch() == 77);
    sp_proto::Header h;
    REQUIRE(sp_proto::read_header(buf + bd.size(), at - bd.size(), h));
    CHECK(h.template_id == sp_proto::RecapRecordDecoder::kTemplateId);
}

TEST_CASE("spp codec: SourceState round trip; Unknown and None are the zero values") {
    uint8_t buf[64];
    sp_proto::SourceStateEncoder e(buf, sizeof(buf));
    e.SourceEpoch(1'700'000'000'000'000'000ull).Instrument(42).Sequence(9001).Venue(3)
        .State(sp_proto::SourceStateCode::Ok).Reason(sp_proto::SourceReason::Resync);
    const size_t n = e.finish();
    REQUIRE(n == 8 + 40);
    sp_proto::SourceStateDecoder d;
    REQUIRE(d.wrap(buf, n));
    CHECK(d.CorrelationId() == 0);
    CHECK(d.SourceEpoch() == 1'700'000'000'000'000'000ull);
    CHECK(d.Instrument() == 42);
    CHECK(d.Sequence() == 9001);
    CHECK(d.Venue() == 3);
    CHECK(d.State() == sp_proto::SourceStateCode::Ok);
    CHECK(d.Reason() == sp_proto::SourceReason::Resync);

    sp_proto::SourceStateEncoder z(buf, sizeof(buf));
    REQUIRE(d.wrap(buf, z.finish()));
    CHECK(d.State() == sp_proto::SourceStateCode::Unknown);
    CHECK(d.Reason() == sp_proto::SourceReason::None);
}

TEST_CASE("spp codec: an older sender's shorter block reads as absent; a newer sender's longer block is skipped") {
    uint8_t buf[128];
    sp_proto::RecapRequestEncoder e(buf, sizeof(buf));
    e.CorrelationId(1).AgentEpoch(2).Instrument(3).LastSeqSeen(4).SourceEpochSeen(5).Venue(6);
    const size_t n = e.finish();
    REQUIRE(n == 8 + 48);

    // Older sender: block of 24 bytes (CorrelationId, AgentEpoch, Instrument only).
    uint8_t old[8 + 24];
    std::memcpy(old, buf, sizeof(old));
    const uint16_t bl = 24;
    std::memcpy(old, &bl, 2);
    sp_proto::RecapRequestDecoder d;
    REQUIRE(d.wrap(old, sizeof(old)));
    CHECK(d.Instrument() == 3);
    CHECK(d.LastSeqSeen() == 0);
    CHECK(d.Venue() == 0);

    // Newer sender: version 2 with 8 more root bytes appended.
    uint8_t neu[8 + 56] = {};
    std::memcpy(neu, buf, n);
    const uint16_t bl2 = 56, v2 = 2;
    std::memcpy(neu, &bl2, 2);
    std::memcpy(neu + 6, &v2, 2);
    REQUIRE(d.wrap(neu, sizeof(neu)));
    CHECK(d.version() == 2);
    CHECK(d.Venue() == 6);
    CHECK(d.size() == sizeof(neu));
}

TEST_CASE("spp codec: the generator refuses what it does not support") {
    const char* nested = R"(<?xml version="1.0"?>
<sbe:messageSchema xmlns:sbe="http://fixprotocol.io/2016/sbe" package="t" id="9" version="1" byteOrder="littleEndian">
  <types>
    <composite name="messageHeader"><type name="blockLength" primitiveType="uint16"/><type name="templateId" primitiveType="uint16"/><type name="schemaId" primitiveType="uint16"/><type name="version" primitiveType="uint16"/></composite>
    <composite name="groupSizeEncoding"><type name="blockLength" primitiveType="uint16"/><type name="numInGroup" primitiveType="uint16"/></composite>
  </types>
  <sbe:message name="M" id="1">
    <field name="A" id="1" type="uint64"/>
    <group name="G" id="2"><field name="B" id="3" type="uint32"/><group name="H" id="4"><field name="C" id="5" type="uint32"/></group></group>
  </sbe:message>
</sbe:messageSchema>)";
    SbeSchema s;
    std::string err;
    REQUIRE_MESSAGE(s.load_string(nested, err), err);
    std::string out;
    CHECK_FALSE(emit_cpp_codec(s, CodecOptions{}, out, err));
    CHECK(err.find("nested") != std::string::npos);
}
