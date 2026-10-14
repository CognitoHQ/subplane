#include <doctest.h>

#include "subplane/decoder/dbn_decoder.h"

#include <cstring>
#include <string>
#include <vector>

using namespace subplane;

namespace {

struct Buf {
    std::vector<uint8_t> b;
    void u8(uint8_t v) { b.push_back(v); }
    void u(uint64_t v, unsigned n) { for (unsigned i = 0; i < n; ++i) b.push_back(static_cast<uint8_t>(v >> (8 * i))); }
    void zeros(size_t n) { b.insert(b.end(), n, 0); }
};

// A DBN record of `total` bytes with the given header fields; body zero except
// a u32 `seq` written at `seq_off` when non-zero.
std::vector<uint8_t> rec(uint32_t total, uint8_t rtype, uint32_t iid, uint64_t ts, uint16_t pub = 1,
                         uint16_t seq_off = 0, uint32_t seq = 0) {
    Buf x;
    x.u8(static_cast<uint8_t>(total / 4)); x.u8(rtype); x.u(pub, 2); x.u(iid, 4); x.u(ts, 8);
    x.zeros(total - 16);
    if (seq_off) std::memcpy(x.b.data() + seq_off, &seq, 4);
    return x.b;
}

struct CountingResolver final : public SymbolResolver {
    uint64_t last = 0; uint16_t last_venue = 0; int calls = 0;
    uint32_t resolve_u64(uint64_t key, uint16_t v) noexcept override { last = key; last_venue = v; ++calls; return static_cast<uint32_t>(key % 100 + 1); }
    uint32_t resolve_str(const char*, uint32_t) noexcept override { ++calls; return 0; }
};

DbnDecoder make(const char* yaml, std::string& err) {
    DbnDecoderConfig cfg;
    DbnDecoder d;
    if (parse_dbn_decoder_config(YAML::Load(yaml), cfg, err)) d.init(cfg, err);
    return d;
}

} // namespace

TEST_CASE("dbn: rtype table") {
    REQUIRE(dbn_rtype_by_name("MBO"));
    CHECK(dbn_rtype_by_name("MBO")->code == 0xA0);
    CHECK(dbn_rtype_by_name("MBO")->sequence_off == 52);
    CHECK(dbn_rtype_by_name("TRADE")->code == 0x00);
    CHECK(dbn_rtype_by_name("MBP_1")->sequence_off == 44);
    CHECK(dbn_rtype_by_name("SYSTEM")->sequence_off == 0);
    CHECK(dbn_rtype_by_code(0x13)->name == std::string("DEFINITION"));
    CHECK_FALSE(dbn_rtype_by_name("NOPE"));
}

TEST_CASE("dbn: default mapping decodes MBO, trade, MBP-1 and SYSTEM") {
    std::string err;
    DbnDecoder d = make("{type: dbn}", err);
    REQUIRE_MESSAGE(err.empty(), err);
    CountingResolver syms;
    TickHeader h{};

    auto mbo = rec(56, 0xA0, 432669, 1758742200000860311ull, 1, 52, 77);
    auto r = d.decode(mbo.data(), static_cast<uint32_t>(mbo.size()), h, syms);
    CHECK(r.status == DecodeStatus::OK);
    CHECK(r.total_len == 56);
    CHECK(h.kind == static_cast<uint8_t>(Kind::DATA));
    CHECK(h.msg_type == static_cast<uint8_t>(MsgType::BOOK_UPDATE));
    CHECK(h.symbol_id == 432669);          // passthrough by default
    CHECK(h.venue_id == 1);
    CHECK(h.exchange_ts == 1758742200000860311ull);
    CHECK(h.sequence == 77);
    CHECK(h.template_id == 0xA0);
    CHECK(h.wire_format == static_cast<uint8_t>(WireFormat::DBN));
    CHECK(syms.calls == 0);

    auto trade = rec(48, 0x00, 7, 9, 2, 44, 123456);
    CHECK(d.decode(trade.data(), 48, h, syms).status == DecodeStatus::OK);
    CHECK(h.msg_type == static_cast<uint8_t>(MsgType::TRADE));
    CHECK(h.sequence == 123456);
    CHECK(h.venue_id == 2);

    auto mbp1 = rec(80, 0x01, 7, 9, 1, 44, 5);
    CHECK(d.decode(mbp1.data(), 80, h, syms).status == DecodeStatus::OK);
    CHECK(h.msg_type == static_cast<uint8_t>(MsgType::QUOTE));
    CHECK(h.sequence == 5);

    auto sys = rec(320, 0x17, 0, 9);
    CHECK(d.decode(sys.data(), 320, h, syms).status == DecodeStatus::OK);
    CHECK(h.kind == static_cast<uint8_t>(Kind::BUS_ADMIN));
    CHECK(h.msg_type == static_cast<uint8_t>(ControlCode::BUS_HEARTBEAT));
    CHECK(h.sequence == 0);
}

TEST_CASE("dbn: unmapped rtype, truncation and length mismatch") {
    std::string err;
    DbnDecoder d = make("{type: dbn}", err);
    REQUIRE(err.empty());
    CountingResolver syms;
    TickHeader h{};

    auto def = rec(520, 0x13, 1, 1);           // DEFINITION: unmapped by default
    auto r = d.decode(def.data(), 520, h, syms);
    CHECK(r.status == DecodeStatus::UNKNOWN_TEMPLATE);
    CHECK(h.template_id == 0x13);              // visible to last_unknown_template

    auto mbo = rec(56, 0xA0, 1, 1);
    CHECK(d.decode(mbo.data(), 15, h, syms).status == DecodeStatus::TRUNCATED);   // shorter than a header
    CHECK(d.decode(mbo.data(), 40, h, syms).status == DecodeStatus::TRUNCATED);   // length byte says 56, only 40 given
    CHECK(d.decode(mbo.data(), 60, h, syms).status == DecodeStatus::MALFORMED);   // 4 trailing bytes: misaligned
    mbo[0] = 2;                                                                    // length 8 < header
    CHECK(d.decode(mbo.data(), 56, h, syms).status == DecodeStatus::TRUNCATED);
}

TEST_CASE("dbn: table symbol mode and explicit mappings") {
    std::string err;
    DbnDecoder d = make(R"({type: dbn, symbol_id_mode: table, messages: [
        {rtype: MBO, msg_type: TRADE}, {rtype: DEFINITION, msg_type: STATUS}, {rtype: 0x17, admin: SEQ_RESET}]})", err);
    REQUIRE_MESSAGE(err.empty(), err);
    CountingResolver syms;
    TickHeader h{};
    auto mbo = rec(56, 0xA0, 432669, 1);
    CHECK(d.decode(mbo.data(), 56, h, syms).status == DecodeStatus::OK);
    CHECK(h.msg_type == static_cast<uint8_t>(MsgType::TRADE));
    CHECK(syms.calls == 1);
    CHECK(syms.last == 432669);
    CHECK(h.symbol_id == 432669 % 100 + 1);
    auto def = rec(520, 0x13, 1, 1);
    CHECK(d.decode(def.data(), 520, h, syms).status == DecodeStatus::OK);
    CHECK(h.msg_type == static_cast<uint8_t>(MsgType::STATUS));
    auto sys = rec(320, 0x17, 0, 1);
    CHECK(d.decode(sys.data(), 320, h, syms).status == DecodeStatus::OK);
    CHECK(h.msg_type == static_cast<uint8_t>(ControlCode::BUS_SEQ_RESET));
    auto trade = rec(48, 0x00, 1, 1);
    CHECK(d.decode(trade.data(), 48, h, syms).status == DecodeStatus::UNKNOWN_TEMPLATE); // not in explicit list
}

TEST_CASE("dbn: config errors") {
    std::string err;
    DbnDecoderConfig cfg;
    CHECK_FALSE(parse_dbn_decoder_config(YAML::Load("{type: dbn, messages: [{rtype: BOGUS, msg_type: TRADE}]}"), cfg, err));
    CHECK(err.find("unknown rtype") != std::string::npos);
    CHECK_FALSE(parse_dbn_decoder_config(YAML::Load("{type: dbn, messages: [{rtype: MBO}]}"), cfg, err));
    CHECK_FALSE(parse_dbn_decoder_config(YAML::Load("{type: dbn, messages: [{rtype: MBO, msg_type: TRADE}, {rtype: 160, msg_type: QUOTE}]}"), cfg, err));
    CHECK(err.find("duplicate") != std::string::npos);
    CHECK_FALSE(parse_dbn_decoder_config(YAML::Load("{type: dbn, schema: x.xml}"), cfg, err));
    CHECK_FALSE(parse_dbn_decoder_config(YAML::Load("{type: dbn, symbol_id_mode: dense}"), cfg, err));
    CHECK(parse_dbn_decoder_config(YAML::Load("{type: dbn}"), cfg, err));
    CHECK(cfg.messages.size() == dbn_default_mappings().size());
}

TEST_CASE("dbn: symbol_scope venue interns with the publisher id") {
    std::string err;
    DbnDecoder d = make(R"({type: dbn, symbol_id_mode: table, symbol_scope: venue})", err);
    REQUIRE_MESSAGE(err.empty(), err);
    CHECK(d.symbol_venue_scope());
    CountingResolver syms;
    TickHeader h{};
    auto mbo = rec(56, 0xA0, 432669, 1, 42);
    REQUIRE(d.decode(mbo.data(), 56, h, syms).status == DecodeStatus::OK);
    CHECK(syms.last == 432669);
    CHECK(syms.last_venue == 42);
    make(R"({type: dbn, symbol_scope: venue})", err); // passthrough is the dbn default
    CHECK(err.find("table") != std::string::npos);
}
