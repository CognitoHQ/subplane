#include <doctest.h>

#include "fb_fixture.h"
#include "subplane/decoder/flatbuffers_decoder.h"

#include <chrono>
#include <cstring>
#include <string>
#include <vector>

using namespace subplane;
using namespace subplane::test;

namespace {

const char* kFixtureCfg = R"(
type: flatbuffers
dispatch: { type_field: { path: [2], type: u8 } }
messages:
  - { type: 1, msg_type: TRADE, symbol: { path: [3, 0, 0], type: u32 }, venue: { path: [3, 0, 1], type: u16 },
      exchange_ts: { path: [1], type: u64 }, sequence: { path: [0], type: u64 } }
  - { type: 2, msg_type: QUOTE, symbol: { path: [3, 0, 0], type: u32 }, venue: { path: [3, 0, 1], type: u16 },
      exchange_ts: { path: [1], type: u64 }, sequence: { path: [0], type: u64 } }
  - { type: 3, admin: HEARTBEAT, exchange_ts: { path: [1], type: u64 }, sequence: { path: [0], type: u64 } }
)";

struct CountingResolver final : public SymbolResolver {
    uint64_t last_u64 = 0; uint16_t last_venue = 0; std::string last_str; int u64_calls = 0, str_calls = 0;
    uint32_t resolve_u64(uint64_t key, uint16_t v) noexcept override { last_u64 = key; last_venue = v; ++u64_calls; return static_cast<uint32_t>(key % 100 + 1); }
    uint32_t resolve_str(const char* s, uint32_t len) noexcept override { last_str.assign(s, len); ++str_calls; return len; }
};

FlatbuffersDecoder make(const std::string& yaml, std::string& err) {
    FlatbuffersDecoderConfig cfg;
    FlatbuffersDecoder d;
    if (parse_flatbuffers_decoder_config(YAML::Load(yaml), cfg, err)) d.init(cfg, err);
    return d;
}

} // namespace

TEST_CASE("flatbuffers: every fixture message routes as the generated code would") {
    std::string err;
    FlatbuffersDecoder d = make(kFixtureCfg, err);
    REQUIRE_MESSAGE(err.empty(), err);
    CHECK(d.symbol_passthrough());
    auto msgs = fb_fixture_messages();
    auto exp = fb_fixture_expected();
    REQUIRE(msgs.size() == 300);
    CountingResolver syms;
    for (size_t i = 0; i < msgs.size(); ++i) {
        TickHeader h{};
        const auto r = d.decode(msgs[i].data(), static_cast<uint32_t>(msgs[i].size()), h, syms);
        REQUIRE(r.status == DecodeStatus::OK);
        CHECK(r.total_len == msgs[i].size());
        CHECK(h.template_id == exp[i].kind);
        CHECK(h.sequence == exp[i].seq);
        CHECK(h.exchange_ts == exp[i].ts);
        CHECK(h.symbol_id == exp[i].instrument);
        CHECK(h.venue_id == exp[i].venue);
        CHECK(h.wire_format == static_cast<uint8_t>(WireFormat::FLATBUFFERS));
        CHECK(h.schema_id == 0);
        if (exp[i].kind == 3) {
            CHECK(h.kind == static_cast<uint8_t>(Kind::BUS_ADMIN));
            CHECK(h.msg_type == static_cast<uint8_t>(ControlCode::BUS_HEARTBEAT));
        } else {
            CHECK(h.kind == static_cast<uint8_t>(Kind::DATA));
            CHECK(h.msg_type == static_cast<uint8_t>(exp[i].kind == 1 ? MsgType::TRADE : MsgType::QUOTE));
        }
    }
    CHECK(syms.u64_calls == 0);

    // Cost: all routing fields, bounds-checked, per message (informational).
    uint64_t acc = 0;
    const int reps = 2000;
    const auto t0 = std::chrono::steady_clock::now();
    for (int r = 0; r < reps; ++r)
        for (const auto& m : msgs) { TickHeader h{}; d.decode(m.data(), static_cast<uint32_t>(m.size()), h, syms); acc += h.symbol_id + h.sequence; }
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
    MESSAGE("flatbuffers decode: " << static_cast<double>(ns) / (static_cast<double>(msgs.size()) * reps) << " ns/msg (acc " << acc << ")");
}

TEST_CASE("flatbuffers: field absent on an old producer reads as 0; table mode resolves u64 or string symbols") {
    std::string err;
    // venue mapped to Envelope.flags (id 4): set to 7 by current producers, absent on every 5th message.
    FlatbuffersDecoder d = make(R"({type: flatbuffers, dispatch: {type_field: {path: [2], type: u8}}, messages: [
        {type: 1, msg_type: TRADE, venue: {path: [4], type: u16}}, {type: 2, msg_type: QUOTE, venue: {path: [4], type: u16}}, {type: 3, admin: HEARTBEAT}]})", err);
    REQUIRE_MESSAGE(err.empty(), err);
    auto msgs = fb_fixture_messages(); auto exp = fb_fixture_expected();
    CountingResolver syms;
    for (size_t i = 0; i < msgs.size(); ++i) {
        TickHeader h{};
        REQUIRE(d.decode(msgs[i].data(), static_cast<uint32_t>(msgs[i].size()), h, syms).status == DecodeStatus::OK);
        CHECK(h.venue_id == exp[i].flags);
        CHECK(h.symbol_id == 0); // no symbol path
    }
    CHECK(d.warnings().size() == 3);

    FlatbuffersDecoder t = make(R"({type: flatbuffers, symbol_id_mode: table, dispatch: {type_field: {path: [2], type: u8}}, messages: [
        {type: 1, msg_type: TRADE, symbol: {path: [3, 0, 2], type: string}}, {type: 2, msg_type: QUOTE, symbol: {path: [3, 0, 0], type: u32}}]})", err);
    REQUIRE_MESSAGE(err.empty(), err);
    CHECK_FALSE(t.symbol_passthrough());
    TickHeader h{};
    REQUIRE(t.decode(msgs[1].data(), static_cast<uint32_t>(msgs[1].size()), h, syms).status == DecodeStatus::OK); // trade
    CHECK(syms.str_calls == 1);
    CHECK(syms.last_str == exp[1].symbol);
    CHECK(h.symbol_id == exp[1].symbol.size());
    REQUIRE(t.decode(msgs[0].data(), static_cast<uint32_t>(msgs[0].size()), h, syms).status == DecodeStatus::OK); // quote
    CHECK(syms.u64_calls == 1);
    CHECK(syms.last_u64 == exp[0].instrument);
    CHECK(h.symbol_id == exp[0].instrument % 100 + 1);
    // Heartbeat is not mapped here: unknown template with the type visible.
    auto r = t.decode(msgs[9].data(), static_cast<uint32_t>(msgs[9].size()), h, syms);
    CHECK(r.status == DecodeStatus::UNKNOWN_TEMPLATE);
    CHECK(h.template_id == 3);
}

TEST_CASE("flatbuffers: identifier dispatch, template_id is the entry position") {
    std::string err;
    FlatbuffersDecoder d = make(R"({type: flatbuffers, dispatch: identifier, messages: [
        {identifier: "XXXX", msg_type: STATUS}, {identifier: "MDv1", msg_type: TRADE, symbol: {path: [3, 0, 0], type: u32}, sequence: {path: [0], type: u64}}]})", err);
    REQUIRE_MESSAGE(err.empty(), err);
    auto msgs = fb_fixture_messages(); auto exp = fb_fixture_expected();
    CountingResolver syms;
    TickHeader h{};
    REQUIRE(d.decode(msgs[1].data(), static_cast<uint32_t>(msgs[1].size()), h, syms).status == DecodeStatus::OK);
    CHECK(h.template_id == 2);
    CHECK(h.msg_type == static_cast<uint8_t>(MsgType::TRADE));
    CHECK(h.symbol_id == exp[1].instrument);
    CHECK(h.sequence == exp[1].seq);
    auto bad = msgs[1];
    std::memcpy(bad.data() + 8, "ZZZZ", 4); // identifier lives after the size prefix and the root offset
    CHECK(d.decode(bad.data(), static_cast<uint32_t>(bad.size()), h, syms).status == DecodeStatus::UNKNOWN_TEMPLATE);
    CHECK(h.template_id == 0);
}

TEST_CASE("flatbuffers: size prefix must account for the whole buffer; short input is truncated") {
    std::string err;
    FlatbuffersDecoder d = make(kFixtureCfg, err);
    REQUIRE(err.empty());
    auto msgs = fb_fixture_messages();
    CountingResolver syms;
    TickHeader h{};
    auto m = msgs[0];
    CHECK(d.decode(m.data(), 7, h, syms).status == DecodeStatus::TRUNCATED);
    CHECK(d.decode(m.data(), static_cast<uint32_t>(m.size() - 10), h, syms).status == DecodeStatus::TRUNCATED);
    m.push_back(0);
    CHECK(d.decode(m.data(), static_cast<uint32_t>(m.size()), h, syms).status == DecodeStatus::MALFORMED);
    // Not size-prefixed: the same bytes minus the prefix are a plain buffer.
    FlatbuffersDecoder p = make(std::string(kFixtureCfg) + "size_prefixed: false\n", err);
    REQUIRE_MESSAGE(err.empty(), err);
    CHECK_FALSE(p.size_prefixed());
    const auto& m0 = msgs[0];
    REQUIRE(p.decode(m0.data() + 4, static_cast<uint32_t>(m0.size() - 4), h, syms).status == DecodeStatus::OK);
    CHECK(h.template_id == 2);
    std::vector<uint8_t> junk(16, 0xFF);
    CHECK(p.decode(junk.data(), 16, h, syms).status == DecodeStatus::MALFORMED);
}

TEST_CASE("flatbuffers: config errors") {
    std::string err;
    FlatbuffersDecoderConfig cfg;
    auto bad = [&](const char* yaml, const char* needle) {
        err.clear();
        CHECK_FALSE(parse_flatbuffers_decoder_config(YAML::Load(yaml), cfg, err));
        CHECK_MESSAGE(err.find(needle) != std::string::npos, err);
    };
    bad("{type: flatbuffers, messages: [{type: 1, msg_type: TRADE}]}", "dispatch is required");
    bad("{type: flatbuffers, dispatch: bogus, messages: [{type: 1, msg_type: TRADE}]}", "dispatch must be");
    bad("{type: flatbuffers, dispatch: {type_field: {path: [2], type: u64}}, messages: [{type: 1, msg_type: TRADE}]}", "u8, u16 or u32");
    bad("{type: flatbuffers, dispatch: {type_field: {path: [1, 2, 3, 4, 5], type: u8}}, messages: [{type: 1, msg_type: TRADE}]}", "path must list");
    bad("{type: flatbuffers, dispatch: identifier, messages: [{type: 1, msg_type: TRADE}]}", "lacks 'identifier'");
    bad("{type: flatbuffers, dispatch: identifier, messages: [{identifier: ABC, msg_type: TRADE}]}", "exactly 4 characters");
    bad("{type: flatbuffers, dispatch: {type_field: {path: [2], type: u8}}, messages: [{identifier: ABCD, type: 1, msg_type: TRADE}]}", "only valid with dispatch: identifier");
    bad("{type: flatbuffers, dispatch: {type_field: {path: [2], type: u8}}, messages: [{type: 1, msg_type: TRADE}, {type: 1, msg_type: QUOTE}]}", "duplicate type 1");
    bad("{type: flatbuffers, dispatch: {type_field: {path: [2], type: u8}}, messages: [{type: 1, msg_type: TRADE, symbol: {path: [3], type: string}}]}", "only valid for symbol in table mode");
    bad("{type: flatbuffers, dispatch: {type_field: {path: [2], type: u8}}, messages: [{type: 1, msg_type: TRADE, venue: {path: [3], type: f32}}]}", "unknown scalar");
    bad("{type: flatbuffers, dispatch: {type_field: {path: [2], type: u8}}, messages: [{type: 1, msg_type: TRADE, admin: HEARTBEAT}]}", "exactly one of");
    bad("{type: flatbuffers, dispatch: {type_field: {path: [2], type: u8}}, messages: [{type: 70000, msg_type: TRADE}]}", "out of range");
    bad("{type: flatbuffers, schema: x.xml, dispatch: identifier, messages: [{identifier: ABCD, msg_type: TRADE}]}", "schema is not used");
    bad("{type: flatbuffers, dispatch: identifier, messages: []}", "non-empty list");
    CHECK(parse_flatbuffers_decoder_config(YAML::Load(kFixtureCfg), cfg, err));
    CHECK(cfg.messages.size() == 3);
    CHECK(cfg.messages[0].symbol.ids.size() == 3);
    CHECK(cfg.messages[0].symbol.ids[2] == 0);
    CHECK(cfg.size_prefixed);
}

TEST_CASE("flatbuffers: symbol_scope venue needs an integer symbol and a venue path") {
    FlatbuffersDecoderConfig c;
    std::string err;
    CHECK_FALSE(parse_flatbuffers_decoder_config(YAML::Load(R"({symbol_id_mode: table, symbol_scope: venue, dispatch: {type_field: {path: [2], type: u8}}, messages: [
        {type: 1, msg_type: TRADE, symbol: {path: [3, 0, 2], type: string}, venue: {path: [3, 0, 1], type: u16}}]})"), c, err));
    CHECK(err.find("integer symbol") != std::string::npos);
    CHECK_FALSE(parse_flatbuffers_decoder_config(YAML::Load(R"({symbol_id_mode: table, symbol_scope: venue, dispatch: {type_field: {path: [2], type: u8}}, messages: [
        {type: 2, msg_type: QUOTE, symbol: {path: [3, 0, 0], type: u32}}]})"), c, err));
    CHECK(err.find("lacks venue") != std::string::npos);
    REQUIRE_MESSAGE(parse_flatbuffers_decoder_config(YAML::Load(R"({symbol_id_mode: table, symbol_scope: venue, dispatch: {type_field: {path: [2], type: u8}}, messages: [
        {type: 2, msg_type: QUOTE, symbol: {path: [3, 0, 0], type: u32}, venue: {path: [3, 0, 1], type: u16}}]})"), c, err), err);
    CHECK(c.venue_scope);
}

TEST_CASE("flatbuffers: recap markers decode as markers and expose their fields") {
    // The fixture's heartbeat (type 3) stands in for a RECAP_BEGIN: its sequence
    // and timestamp fields play request_id and as_of.
    const char* y = R"(
type: flatbuffers
dispatch: { type_field: { path: [2], type: u8 } }
messages:
  - { type: 1, msg_type: TRADE, symbol: { path: [3, 0, 0], type: u32 }, sequence: { path: [0], type: u64 } }
  - { type: 3, admin: RECAP_BEGIN, fields: { request_id: { path: [0], type: u64 }, as_of: { path: [1], type: u64 } } }
)";
    std::string err;
    FlatbuffersDecoder d = make(y, err);
    REQUIRE_MESSAGE(err.empty(), err);
    CHECK(d.warnings().empty());
    auto msgs = fb_fixture_messages();
    auto exp = fb_fixture_expected();
    CountingResolver syms;
    int markers = 0;
    for (size_t i = 0; i < msgs.size(); ++i) {
        TickHeader h{};
        const auto r = d.decode(msgs[i].data(), static_cast<uint32_t>(msgs[i].size()), h, syms);
        RecapMarkerFields mf;
        const bool is_marker = d.read_recap_fields(msgs[i].data(), static_cast<uint32_t>(msgs[i].size()), mf);
        if (exp[i].kind == 3) {
            ++markers;
            REQUIRE(r.status == DecodeStatus::OK);
            CHECK(r.marker == RecapMarker::BEGIN);
            CHECK(h.kind == static_cast<uint8_t>(Kind::BUS_ADMIN));
            REQUIRE(is_marker);
            CHECK(mf.value[RECAP_REQUEST_ID] == exp[i].seq);
            CHECK(mf.value[RECAP_AS_OF] == exp[i].ts);
            CHECK_FALSE(mf.has[RECAP_REASON]);
        } else {
            CHECK(r.marker == RecapMarker::NONE);
            CHECK_FALSE(is_marker);
        }
    }
    CHECK(markers > 0);

    FlatbuffersDecoderConfig cfg;
    CHECK_FALSE(parse_flatbuffers_decoder_config(YAML::Load("{dispatch: {type_field: {path: [2], type: u8}}, messages: [{type: 3, admin: RECAP_END}]}"), cfg, err));
    CHECK_MESSAGE(err.find("needs 'fields'") != std::string::npos, err);
    CHECK_FALSE(parse_flatbuffers_decoder_config(YAML::Load("{dispatch: {type_field: {path: [2], type: u8}}, messages: [{type: 3, admin: RECAP_BEGIN, fields: {request_id: {path: [0], type: u64}}}]}"), cfg, err));
    CHECK_MESSAGE(err.find("'as_of' is required") != std::string::npos, err);
    CHECK_FALSE(parse_flatbuffers_decoder_config(YAML::Load("{dispatch: {type_field: {path: [2], type: u8}}, messages: [{type: 3, admin: RECAP_END, fields: {request_id: {path: [0], type: string}}}]}"), cfg, err));
    CHECK_FALSE(parse_flatbuffers_decoder_config(YAML::Load("{dispatch: {type_field: {path: [2], type: u8}}, messages: [{type: 1, msg_type: TRADE, fields: {request_id: {path: [0], type: u64}}}]}"), cfg, err));
    CHECK_MESSAGE(err.find("only valid on RECAP_BEGIN") != std::string::npos, err);
}
