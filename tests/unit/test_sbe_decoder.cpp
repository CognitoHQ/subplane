#include <doctest.h>

#include <cstdio>
#include <string>
#include <vector>

#include "subplane/decoder/sbe_routing_decoder.h"

using namespace subplane;

namespace {
std::string sample_path() {
    std::string f = __FILE__;
    return f.substr(0, f.find_last_of('/')) + "/../data/sample_md.xml";
}
std::string read_sample() {
    std::string xml;
    FILE* fp = std::fopen(sample_path().c_str(), "rb");
    REQUIRE(fp);
    char c;
    while (std::fread(&c, 1, 1, fp) == 1) xml.push_back(c);
    std::fclose(fp);
    return xml;
}

const char* kConfig = R"(
type: sbe_routing
schema: schemas/sample_md.xml
fields:
  symbol:      SecurityID
  sequence:    SeqNum
  exchange_ts: TransactTime
  venue:       MarketSegmentID
symbol_id_mode: table
messages:
  - { template: 1,   msg_type: TRADE }
  - { template: 2,   msg_type: QUOTE }
  - { template: 3,   msg_type: BOOK_UPDATE }
  - { template: 100, admin: HEARTBEAT }
  - { template: 101, admin: SEQ_RESET }
)";

struct Buf {
    std::vector<uint8_t> b;
    bool le = true;
    void u(uint64_t v, unsigned size) {
        for (unsigned i = 0; i < size; ++i) {
            unsigned sh = le ? i : size - 1 - i;
            b.push_back(static_cast<uint8_t>(v >> (8 * sh)));
        }
    }
    void zeros(size_t n) { b.insert(b.end(), n, 0); }
    void header(uint16_t blk, uint16_t tid, uint16_t sid = 7) { u(blk, 2); u(tid, 2); u(sid, 2); u(2, 2); }
    void routing(uint64_t sec, uint64_t seq, uint64_t ts, uint16_t seg) { u(sec, 8); u(seq, 8); u(ts, 8); u(seg, 2); }
};

struct FakeResolver : SymbolResolver {
    uint64_t last_key = 0;
    uint16_t last_venue = 0;
    std::string last_str;
    int calls = 0;
    uint32_t resolve_u64(uint64_t k, uint16_t v) noexcept override { ++calls; last_key = k; last_venue = v; return static_cast<uint32_t>(k + 1000); }
    uint32_t resolve_str(const char* s, uint32_t n) noexcept override {
        ++calls; last_str.assign(s, n); return 5000 + n;
    }
};

struct Fixture {
    SbeSchema schema;
    DecoderConfig cfg;
    SbeRoutingDecoder dec;
    FakeResolver res;
    Fixture(const std::string& xml = read_sample(), const char* yaml = kConfig) {
        std::string err;
        REQUIRE_MESSAGE(schema.load_string(xml, err), err);
        REQUIRE_MESSAGE(parse_decoder_config(YAML::Load(yaml), cfg, err), err);
        REQUIRE_MESSAGE(dec.init(cfg, schema, err), err);
    }
    DecodeResult run(const Buf& x, TickHeader& h, uint32_t avail) { return dec.decode(x.b.data(), avail, h, res); }
    DecodeResult run(const Buf& x, TickHeader& h) { return run(x, h, static_cast<uint32_t>(x.b.size())); }
};

Buf trade(bool le = true) {
    Buf x; x.le = le;
    x.header(39, 1);
    x.routing(42, 777, 1700000000123456789ull, 9);
    x.zeros(39 - 26);
    return x;
}
} // namespace

TEST_CASE("decoder config parsing") {
    DecoderConfig c;
    std::string err;
    REQUIRE_MESSAGE(parse_decoder_config(YAML::Load(kConfig), c, err), err);
    CHECK(c.schema_path == "schemas/sample_md.xml");
    CHECK(c.fields.symbol == "SecurityID");
    CHECK(c.fields.venue == "MarketSegmentID");
    CHECK(c.symbol_mode == SymbolIdMode::TABLE);
    REQUIRE(c.messages.size() == 5);
    CHECK_FALSE(c.messages[0].is_admin);
    CHECK(c.messages[0].msg_type == static_cast<uint8_t>(MsgType::TRADE));
    CHECK(c.messages[3].is_admin);
    CHECK(c.messages[3].admin_code == static_cast<uint8_t>(ControlCode::BUS_HEARTBEAT));

    REQUIRE(parse_decoder_config(YAML::Load("{schema: a.xml, messages: [{template: 10, msg_type: DEFINITION}]}"), c, err));
    CHECK(c.messages[0].msg_type == static_cast<uint8_t>(MsgType::DEFINITION));
    CHECK_FALSE(parse_decoder_config(YAML::Load("{schema: a.xml, messages: [{template: 1, msg_type: NOPE}]}"), c, err));
    CHECK_FALSE(parse_decoder_config(YAML::Load("{messages: [{template: 1, msg_type: TRADE}]}"), c, err));
    CHECK_FALSE(parse_decoder_config(YAML::Load("{schema: a, symbol_id_mode: x, messages: [{template: 1, msg_type: TRADE}]}"), c, err));
    CHECK_FALSE(parse_decoder_config(YAML::Load("{schema: a, messages: [{template: 1, msg_type: TRADE, admin: HEARTBEAT}]}"), c, err));
    CHECK_FALSE(parse_decoder_config(YAML::Load("{schema: a, messages: [{template: 1, msg_type: TRADE}, {template: 1, msg_type: QUOTE}]}"), c, err));
}

TEST_CASE("decoder: trade routing fields") {
    Fixture f;
    TickHeader h{};
    Buf x = trade();
    DecodeResult r = f.run(x, h);
    REQUIRE(r.status == DecodeStatus::OK);
    CHECK(r.total_len == 47);
    CHECK(h.template_id == 1);
    CHECK(h.schema_id == 7);
    CHECK(h.schema_version == 2);
    CHECK(h.wire_format == static_cast<uint8_t>(WireFormat::SBE));
    CHECK(h.kind == static_cast<uint8_t>(Kind::DATA));
    CHECK(h.msg_type == static_cast<uint8_t>(MsgType::TRADE));
    CHECK(h.sequence == 777);
    CHECK(h.exchange_ts == 1700000000123456789ull);
    CHECK(h.venue_id == 9);
    CHECK(h.symbol_id == 1042);
    CHECK(f.res.last_key == 42);
    CHECK(f.dec.warnings().empty());
}

TEST_CASE("decoder: quote") {
    Fixture f;
    Buf x;
    x.header(56, 2);
    x.routing(5, 6, 7, 8);
    x.zeros(56 - 26);
    TickHeader h{};
    auto r = f.run(x, h);
    REQUIRE(r.status == DecodeStatus::OK);
    CHECK(r.total_len == 64);
    CHECK(h.msg_type == static_cast<uint8_t>(MsgType::QUOTE));
    CHECK(h.sequence == 6);
    CHECK(h.venue_id == 8);
}

TEST_CASE("decoder: book update total_len includes groups") {
    Fixture f;
    Buf x;
    x.header(26, 3);
    x.routing(11, 22, 33, 4);
    x.u(13, 2); x.u(2, 2);
    for (int i = 0; i < 2; ++i) { x.zeros(13); x.u(12, 2); x.u(1, 2); x.zeros(12); }
    x.u(4, 2); x.zeros(4);
    x.zeros(30); // following message bytes
    const uint32_t expect = 8 + 26 + 4 + 2 * (13 + 4 + 12) + 2 + 4;
    TickHeader h{};
    auto r = f.run(x, h);
    REQUIRE(r.status == DecodeStatus::OK);
    CHECK(r.total_len == expect);
    CHECK(h.msg_type == static_cast<uint8_t>(MsgType::BOOK_UPDATE));
    CHECK(h.sequence == 22);
}

TEST_CASE("decoder: string symbol and warnings") {
    const char* yaml = R"(
schema: x
fields: { symbol: Symbol, sequence: SeqNum, exchange_ts: TransactTime }
messages:
  - { template: 4, msg_type: TRADE }
  - { template: 1, msg_type: TRADE }
)";
    Fixture f(read_sample(), yaml);
    // Trade has no field named Symbol -> warning, not error
    CHECK_FALSE(f.dec.warnings().empty());
    Buf x;
    x.header(24, 4);
    const char s[8] = {'A', 'B', 'C', 0, 0, 0, 0, 0};
    for (char c : s) x.b.push_back(static_cast<uint8_t>(c));
    x.u(99, 8); x.u(123, 8);
    TickHeader h{};
    auto r = f.run(x, h);
    REQUIRE(r.status == DecodeStatus::OK);
    CHECK(f.res.last_str == "ABC");
    CHECK(h.symbol_id == 5003);
    CHECK(h.sequence == 99);
    CHECK(h.exchange_ts == 123);
    CHECK(h.venue_id == 0);

    // missing field in a template leaves it 0
    Buf t = trade();
    r = f.run(t, h);
    REQUIRE(r.status == DecodeStatus::OK);
    CHECK(h.symbol_id == 0);
    CHECK(h.sequence == 777);
}

TEST_CASE("decoder: passthrough") {
    std::string y = kConfig;
    y.replace(y.find("table"), 5, "passthrough");
    Fixture f(read_sample(), y.c_str());
    Buf x = trade();
    TickHeader h{};
    REQUIRE(f.run(x, h).status == DecodeStatus::OK);
    CHECK(h.symbol_id == 42);
    CHECK(f.res.calls == 0);
}

TEST_CASE("decoder: admin heartbeat") {
    Fixture f;
    Buf x;
    x.header(8, 100);
    x.u(555, 8);
    TickHeader h{};
    auto r = f.run(x, h);
    REQUIRE(r.status == DecodeStatus::OK);
    CHECK(r.total_len == 16);
    CHECK(h.kind == static_cast<uint8_t>(Kind::BUS_ADMIN));
    CHECK(h.msg_type == static_cast<uint8_t>(ControlCode::BUS_HEARTBEAT));
    CHECK(h.sequence == 555);
    CHECK(h.symbol_id == 0);
    CHECK(f.res.calls == 0);
}

TEST_CASE("decoder: error statuses") {
    Fixture f;
    TickHeader h{};
    Buf u; u.header(4, 55); u.zeros(4);
    auto r = f.run(u, h);
    CHECK(r.status == DecodeStatus::UNKNOWN_TEMPLATE);
    CHECK(r.total_len == 0);

    // template 4 exists in the schema but is not mapped -> unknown
    Buf tk; tk.header(24, 4); tk.zeros(24);
    CHECK(f.run(tk, h).status == DecodeStatus::UNKNOWN_TEMPLATE);

    Buf m = trade(); // wrong schema id
    m.b[4] = 8;
    CHECK(f.run(m, h).status == DecodeStatus::SCHEMA_MISMATCH);

    Buf t = trade();
    CHECK(f.run(t, h, 5).status == DecodeStatus::TRUNCATED);
    CHECK(f.run(t, h, 30).status == DecodeStatus::TRUNCATED);

    Buf b; b.header(26, 3); b.routing(1, 2, 3, 4); b.u(13, 2); b.u(0xFFFF, 2); b.u(0, 2);
    CHECK(f.run(b, h).status == DecodeStatus::MALFORMED);
}

TEST_CASE("decoder: nested routing field is a config error") {
    const char* yaml = R"(
schema: x
fields: { symbol: SecurityID, sequence: Px }
messages:
  - { template: 3, msg_type: BOOK_UPDATE }
)";
    SbeSchema schema;
    std::string err;
    REQUIRE(schema.load_string(read_sample(), err));
    DecoderConfig cfg;
    REQUIRE(parse_decoder_config(YAML::Load(yaml), cfg, err));
    SbeRoutingDecoder d;
    CHECK_FALSE(d.init(cfg, schema, err));
    CHECK(err.find("Px") != std::string::npos);
    CHECK(err.find("BookUpdate") != std::string::npos);
    CHECK(err.find("inside a group/data") != std::string::npos);

    // template not in schema
    cfg.fields.sequence = "SeqNum";
    cfg.messages[0].template_id = 77;
    CHECK_FALSE(d.init(cfg, schema, err));

    // passthrough with char symbol
    cfg.messages[0].template_id = 4;
    cfg.fields.symbol = "Symbol";
    cfg.symbol_mode = SymbolIdMode::PASSTHROUGH;
    CHECK_FALSE(d.init(cfg, schema, err));
}

TEST_CASE("decoder: big endian") {
    std::string xml = read_sample();
    xml.replace(xml.find("littleEndian"), 12, "bigEndian");
    Fixture f(xml);
    Buf x = trade(false);
    TickHeader h{};
    auto r = f.run(x, h);
    REQUIRE(r.status == DecodeStatus::OK);
    CHECK(r.total_len == 47);
    CHECK(h.sequence == 777);
    CHECK(h.exchange_ts == 1700000000123456789ull);
    CHECK(h.venue_id == 9);
    CHECK(h.symbol_id == 1042);
    CHECK(h.schema_version == 2);
}

TEST_CASE("decoder: symbol_scope venue passes the venue to the resolver") {
    std::string cfg = kConfig;
    cfg.replace(cfg.find("symbol_id_mode: table"), 21, "symbol_id_mode: table\nsymbol_scope: venue");
    Fixture f(read_sample(), cfg.c_str());
    TickHeader h{};
    REQUIRE(f.run(trade(), h).status == DecodeStatus::OK);
    CHECK(f.res.last_key == 42);
    CHECK(f.res.last_venue == 9);

    Fixture g; // global scope: venue 0
    REQUIRE(g.run(trade(), h).status == DecodeStatus::OK);
    CHECK(g.res.last_venue == 0);

    DecoderConfig c;
    std::string err;
    CHECK_FALSE(parse_decoder_config(YAML::Load("{schema: a, fields: {symbol: S, venue: V}, symbol_scope: nope, messages: [{template: 1, msg_type: TRADE}]}"), c, err));
    CHECK_FALSE(parse_decoder_config(YAML::Load("{schema: a, fields: {symbol: S}, symbol_scope: venue, messages: [{template: 1, msg_type: TRADE}]}"), c, err));
    CHECK(err.find("fields.venue") != std::string::npos);
    CHECK_FALSE(parse_decoder_config(YAML::Load("{schema: a, fields: {symbol: S, venue: V}, symbol_id_mode: passthrough, symbol_scope: venue, messages: [{template: 1, msg_type: TRADE}]}"), c, err));
    CHECK(err.find("table") != std::string::npos);
}

TEST_CASE("decoder: recap markers decode as markers and expose their fields") {
    const char* y = R"(
type: sbe_routing
schema: schemas/sample_md.xml
fields: { symbol: SecurityID, sequence: SeqNum }
messages:
  - { template: 1,   msg_type: TRADE }
  - { template: 101, admin: RECAP_BEGIN,  fields: { request_id: SeqNum, as_of: NewSeq } }
  - { template: 100, admin: RECAP_END,    fields: { request_id: SeqNum } }
  - { template: 4,   admin: RECAP_REJECT, fields: { request_id: SeqNum, reason: TransactTime } }
)";
    Fixture f(read_sample(), y);
    REQUIRE(f.cfg.messages[1].marker == RecapMarker::BEGIN);
    CHECK(f.cfg.messages[1].is_admin);

    Buf b;
    b.header(16, 101);
    b.u(77, 8);   // SeqNum -> request_id
    b.u(5000, 8); // NewSeq -> as_of
    TickHeader h{};
    auto r = f.run(b, h);
    REQUIRE(r.status == DecodeStatus::OK);
    CHECK(r.marker == RecapMarker::BEGIN);
    CHECK(h.kind == static_cast<uint8_t>(Kind::BUS_ADMIN));
    RecapMarkerFields mf;
    REQUIRE(f.dec.read_recap_fields(b.b.data(), static_cast<uint32_t>(b.b.size()), mf));
    CHECK(mf.has[RECAP_REQUEST_ID]);
    CHECK(mf.value[RECAP_REQUEST_ID] == 77);
    CHECK(mf.has[RECAP_AS_OF]);
    CHECK(mf.value[RECAP_AS_OF] == 5000);
    CHECK_FALSE(mf.has[RECAP_SOURCE_EPOCH]);

    // A shorter block from an older publisher: as_of is beyond it, so absent.
    Buf old;
    old.header(8, 101);
    old.u(78, 8);
    REQUIRE(f.dec.read_recap_fields(old.b.data(), static_cast<uint32_t>(old.b.size()), mf));
    CHECK(mf.value[RECAP_REQUEST_ID] == 78);
    CHECK_FALSE(mf.has[RECAP_AS_OF]);

    Buf e;
    e.header(8, 100);
    e.u(77, 8);
    CHECK(f.run(e, h).marker == RecapMarker::END);

    // Not a marker: decodes as before, and has no recap fields.
    Buf t = trade();
    r = f.run(t, h);
    REQUIRE(r.status == DecodeStatus::OK);
    CHECK(r.marker == RecapMarker::NONE);
    CHECK_FALSE(f.dec.read_recap_fields(t.b.data(), static_cast<uint32_t>(t.b.size()), mf));
}

TEST_CASE("decoder: recap marker config errors") {
    DecoderConfig c;
    std::string err;
    auto bad = [&](const char* yaml, const char* expect) {
        CHECK_FALSE(parse_decoder_config(YAML::Load(yaml), c, err));
        CHECK_MESSAGE(err.find(expect) != std::string::npos, err);
    };
    bad("{schema: a, messages: [{template: 1, msg_type: TRADE, fields: {request_id: SeqNum}}]}", "only valid on RECAP_BEGIN");
    bad("{schema: a, messages: [{template: 1, admin: HEARTBEAT, fields: {request_id: SeqNum}}]}", "only valid on RECAP_BEGIN");
    bad("{schema: a, messages: [{template: 1, admin: RECAP_END}]}", "needs 'fields'");
    bad("{schema: a, messages: [{template: 1, admin: RECAP_END, fields: {as_of: X}}]}", "'request_id' is required");
    bad("{schema: a, messages: [{template: 1, admin: RECAP_BEGIN, fields: {request_id: X}}]}", "'as_of' is required");
    bad("{schema: a, messages: [{template: 1, admin: RECAP_END, fields: {request_id: X, nope: Y}}]}", "unknown recap field 'nope'");

    // Field checks need the schema: a char array, and a field the message lacks.
    SbeSchema schema;
    REQUIRE(schema.load_string(read_sample(), err));
    SbeRoutingDecoder dec;
    REQUIRE(parse_decoder_config(YAML::Load("{schema: a, messages: [{template: 4, admin: RECAP_END, fields: {request_id: Symbol}}]}"), c, err));
    CHECK_FALSE(dec.init(c, schema, err));
    CHECK_MESSAGE(err.find("must be an integer field") != std::string::npos, err);
    REQUIRE(parse_decoder_config(YAML::Load("{schema: a, messages: [{template: 100, admin: RECAP_END, fields: {request_id: NewSeq}}]}"), c, err));
    CHECK_FALSE(dec.init(c, schema, err));
}
