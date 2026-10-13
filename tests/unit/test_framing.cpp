#include <doctest.h>

#include "subplane/decoder/sbe_schema.h"
#include "subplane/framing/framing.h"
#include "subplane/framing/sbe_length.h"

#include <cstring>
#include <string>
#include <vector>

using namespace subplane;

namespace {

std::string sample_path() {
    std::string f = __FILE__;
    return f.substr(0, f.find_last_of('/')) + "/../data/sample_md.xml";
}

struct Fixture {
    SbeSchema schema;
    SbeLengthWalker walker;
    Fixture() {
        std::string err;
        REQUIRE_MESSAGE(schema.load_file(sample_path(), err), err);
        REQUIRE_MESSAGE(walker.build(schema, err), err);
    }
};

void put_u16le(std::vector<uint8_t>& b, uint16_t v) { b.push_back(static_cast<uint8_t>(v)); b.push_back(static_cast<uint8_t>(v >> 8)); }
void put_u16be(std::vector<uint8_t>& b, uint16_t v) { b.push_back(static_cast<uint8_t>(v >> 8)); b.push_back(static_cast<uint8_t>(v)); }
void put_u64le(std::vector<uint8_t>& b, uint64_t v) { for (int i = 0; i < 8; ++i) b.push_back(static_cast<uint8_t>(v >> (8 * i))); }
void put_u64be(std::vector<uint8_t>& b, uint64_t v) { for (int i = 7; i >= 0; --i) b.push_back(static_cast<uint8_t>(v >> (8 * i))); }

// Heartbeat (template 100): header(8) + SeqNum u64 => 16 bytes.
std::vector<uint8_t> heartbeat(uint64_t seq, uint16_t schema_id = 7, uint16_t version = 2) {
    std::vector<uint8_t> m;
    put_u16le(m, 8);      // blockLength
    put_u16le(m, 100);    // templateId
    put_u16le(m, schema_id);
    put_u16le(m, version);
    put_u64le(m, seq);
    return m;
}

std::vector<uint8_t> unknown_template_msg() {
    std::vector<uint8_t> m;
    put_u16le(m, 4);
    put_u16le(m, 999);
    put_u16le(m, 7);
    put_u16le(m, 2);
    put_u16le(m, 0);
    put_u16le(m, 0);
    return m;
}

PacketView view(const std::vector<uint8_t>& b) { return PacketView{b.data(), static_cast<uint32_t>(b.size()), 1}; }

} // namespace

TEST_CASE("FramingConfig: defaults, offsets, references and errors") {
    FramingConfig c;
    std::string err;
    CHECK(FramingConfig::parse(YAML::Node(), c, err));
    CHECK(c.len_source == LenSource::SCHEMA);
    CHECK(c.seq_source == SeqSource::NONE);
    CHECK(c.packet_header_size == 0);

    CHECK(FramingConfig::parse(YAML::Load(R"(
packet_header:
  - { name: session, type: bytes, size: 10 }
  - { name: seq,     type: u64be }
  - { name: count,   type: u16be }
message_prefix:
  - { name: len, type: u16be }
sequence: packet_header.seq
count: packet_header.count
length: message_prefix.len
)"), c, err));
    CHECK(c.packet_header_size == 20);
    CHECK(c.packet_header[1].offset == 10);
    CHECK(c.packet_header[2].offset == 18);
    CHECK(c.message_prefix_size == 2);
    CHECK(c.seq_source == SeqSource::PACKET_HEADER);
    CHECK(c.has_count);
    CHECK(c.len_source == LenSource::MESSAGE_PREFIX);

    CHECK(FramingConfig::parse(YAML::Load("{sequence: message.SeqNum}"), c, err));
    CHECK(c.seq_source == SeqSource::MESSAGE_FIELD);
    CHECK(c.seq_field == "SeqNum");

    CHECK_FALSE(FramingConfig::parse(YAML::Load("{sequence: packet_header.nope}"), c, err));
    CHECK_FALSE(FramingConfig::parse(YAML::Load("{length: message_prefix.nope}"), c, err));
    CHECK_FALSE(FramingConfig::parse(YAML::Load("{packet_header: [{name: x, type: u99}]}"), c, err));
    CHECK_FALSE(FramingConfig::parse(YAML::Load("{packet_header: [{name: x, type: u8}, {name: x, type: u8}]}"), c, err));
    CHECK_FALSE(FramingConfig::parse(YAML::Load("{packet_header: [{name: s, type: bytes}]}"), c, err));

    // explicit offset
    CHECK(FramingConfig::parse(YAML::Load("{packet_header: [{name: a, type: u8}, {name: b, type: u32le, offset: 4}]}"), c, err));
    CHECK(c.packet_header_size == 8);
}

TEST_CASE("Framer: bare SBE concatenation, length from schema walk") {
    Fixture fx;
    FramingConfig c;
    std::string err;
    REQUIRE(FramingConfig::parse(YAML::Node(), c, err));
    Framer fr;
    REQUIRE_MESSAGE(fr.init(c, &fx.walker, err), err);

    std::vector<uint8_t> pkt;
    for (uint64_t s = 10; s < 13; ++s) {
        auto m = heartbeat(s);
        pkt.insert(pkt.end(), m.begin(), m.end());
    }
    FrameInfo info;
    MessageIter it;
    REQUIRE(fr.parse(view(pkt), info, it));
    CHECK_FALSE(info.has_seq);
    const uint8_t* msg = nullptr;
    uint32_t len = 0;
    int n = 0;
    while (fr.next(it, msg, len) == NextStatus::OK) {
        CHECK(len == 16);
        CHECK(msg == pkt.data() + 16 * n);
        ++n;
    }
    CHECK(n == 3);
    CHECK(fr.next(it, msg, len) == NextStatus::END);
}

TEST_CASE("Framer: schema length with an unknown template is UNDECODABLE, truncated tail is TRUNCATED") {
    Fixture fx;
    FramingConfig c;
    std::string err;
    REQUIRE(FramingConfig::parse(YAML::Node(), c, err));
    Framer fr;
    REQUIRE(fr.init(c, &fx.walker, err));

    std::vector<uint8_t> pkt = heartbeat(1);
    auto u = unknown_template_msg();
    pkt.insert(pkt.end(), u.begin(), u.end());
    FrameInfo info;
    MessageIter it;
    REQUIRE(fr.parse(view(pkt), info, it));
    const uint8_t* msg = nullptr;
    uint32_t len = 0;
    CHECK(fr.next(it, msg, len) == NextStatus::OK);
    CHECK(fr.next(it, msg, len) == NextStatus::UNDECODABLE);

    std::vector<uint8_t> cut = heartbeat(1);
    cut.resize(12);
    REQUIRE(fr.parse(view(cut), info, it));
    CHECK(fr.next(it, msg, len) == NextStatus::TRUNCATED);
}

TEST_CASE("Framer: MoldUDP64-style header with seq/count and length-prefixed messages (big endian)") {
    Fixture fx;
    FramingConfig c;
    std::string err;
    REQUIRE(FramingConfig::parse(YAML::Load(R"(
packet_header:
  - { name: session, type: bytes, size: 10 }
  - { name: seq,     type: u64be }
  - { name: count,   type: u16be }
message_prefix:
  - { name: len, type: u16be }
sequence: packet_header.seq
count: packet_header.count
length: message_prefix.len
)"), c, err));
    Framer fr;
    REQUIRE(fr.init(c, nullptr, err)); // no walker needed with a prefix

    std::vector<uint8_t> pkt(10, 'S');
    put_u64be(pkt, 123456789);
    put_u16be(pkt, 2);
    auto m1 = heartbeat(5);
    auto m2 = unknown_template_msg(); // unknown is fine: prefix gives the length
    put_u16be(pkt, static_cast<uint16_t>(m1.size()));
    pkt.insert(pkt.end(), m1.begin(), m1.end());
    put_u16be(pkt, static_cast<uint16_t>(m2.size()));
    pkt.insert(pkt.end(), m2.begin(), m2.end());
    // trailing garbage beyond count must be ignored
    pkt.push_back(0xFF);
    pkt.push_back(0xFF);

    FrameInfo info;
    MessageIter it;
    REQUIRE(fr.parse(view(pkt), info, it));
    CHECK(info.has_seq);
    CHECK(info.seq == 123456789);
    CHECK(info.has_count);
    CHECK(info.count == 2);

    const uint8_t* msg = nullptr;
    uint32_t len = 0;
    REQUIRE(fr.next(it, msg, len) == NextStatus::OK);
    CHECK(len == m1.size());
    CHECK(std::memcmp(msg, m1.data(), m1.size()) == 0);
    REQUIRE(fr.next(it, msg, len) == NextStatus::OK);
    CHECK(len == m2.size());
    CHECK(fr.next(it, msg, len) == NextStatus::END);

    // short packet header
    std::vector<uint8_t> shortpkt(5, 0);
    CHECK_FALSE(fr.parse(view(shortpkt), info, it));

    // declared length beyond packet
    std::vector<uint8_t> bad(10, 'S');
    put_u64be(bad, 1);
    put_u16be(bad, 1);
    put_u16be(bad, 500);
    bad.push_back(0);
    REQUIRE(fr.parse(view(bad), info, it));
    CHECK(fr.next(it, msg, len) == NextStatus::TRUNCATED);
}

TEST_CASE("Framer: CME-style header (u32le seq + u64le time) with length_includes_prefix") {
    Fixture fx;
    FramingConfig c;
    std::string err;
    REQUIRE(FramingConfig::parse(YAML::Load(R"(
packet_header:
  - { name: seq,  type: u32le }
  - { name: time, type: u64le }
message_prefix:
  - { name: size, type: u16le }
sequence: packet_header.seq
length: message_prefix.size
length_includes_prefix: true
)"), c, err));
    Framer fr;
    REQUIRE(fr.init(c, &fx.walker, err));

    std::vector<uint8_t> pkt;
    pkt.push_back(42); pkt.push_back(0); pkt.push_back(0); pkt.push_back(0);
    put_u64le(pkt, 999);
    auto m = heartbeat(7);
    put_u16le(pkt, static_cast<uint16_t>(m.size() + 2));
    pkt.insert(pkt.end(), m.begin(), m.end());

    FrameInfo info;
    MessageIter it;
    REQUIRE(fr.parse(view(pkt), info, it));
    CHECK(info.seq == 42);
    const uint8_t* msg = nullptr;
    uint32_t len = 0;
    REQUIRE(fr.next(it, msg, len) == NextStatus::OK);
    CHECK(len == 16);
    CHECK(fr.next(it, msg, len) == NextStatus::END);
}

TEST_CASE("Framer: init requires a walker when length comes from the schema") {
    FramingConfig c;
    std::string err;
    REQUIRE(FramingConfig::parse(YAML::Node(), c, err));
    Framer fr;
    CHECK_FALSE(fr.init(c, nullptr, err));
}

TEST_CASE("Framer: keep_prefix delivers each message with its prefix (size-prefixed FlatBuffers style)") {
    FramingConfig c;
    std::string err;
    REQUIRE(FramingConfig::parse(YAML::Load("{message_prefix: [{name: size, type: u32le}], length: message_prefix.size, keep_prefix: true}"), c, err));
    CHECK(c.keep_prefix);
    Framer fr;
    REQUIRE(fr.init(c, nullptr, err));
    std::vector<uint8_t> pkt = {4, 0, 0, 0, 'A', 'A', 'A', 'A', 2, 0, 0, 0, 'B', 'B'};
    FrameInfo info;
    MessageIter it;
    REQUIRE(fr.parse(view(pkt), info, it));
    const uint8_t* msg = nullptr;
    uint32_t len = 0;
    REQUIRE(fr.next(it, msg, len) == NextStatus::OK);
    CHECK(msg == pkt.data());
    CHECK(len == 8);
    REQUIRE(fr.next(it, msg, len) == NextStatus::OK);
    CHECK(msg == pkt.data() + 8);
    CHECK(len == 6);
    CHECK(fr.next(it, msg, len) == NextStatus::END);
    CHECK_FALSE(FramingConfig::parse(YAML::Load("{length: packet, keep_prefix: true}"), c, err));
    CHECK(err.find("keep_prefix") != std::string::npos);
}
