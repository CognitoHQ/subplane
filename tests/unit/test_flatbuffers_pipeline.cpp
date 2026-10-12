// Real flatc buffers through the real pipeline with a fake transport: default
// framing (one buffer per packet) and message_prefix + keep_prefix (several
// per packet); the ring holds the publisher's whole buffer, prefix included.

#include <doctest.h>

#include "fake_transport.h"
#include "fb_fixture.h"
#include "flashtick/client/shm_reader.h"
#include "flashtick/core/clock.h"
#include "flashtick/pipeline/pipeline.h"
#include "flashtick/shm/control_events.h"

#include <cstring>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

using namespace flashtick;
using namespace flashtick::test;

namespace {

const char* kDecoder = R"(
decoder:
  type: flatbuffers
  dispatch: { type_field: { path: [2], type: u8 } }
  messages:
    - { type: 1, msg_type: TRADE, symbol: { path: [3, 0, 0], type: u32 }, venue: { path: [3, 0, 1], type: u16 },
        exchange_ts: { path: [1], type: u64 }, sequence: { path: [0], type: u64 } }
    - { type: 2, msg_type: QUOTE, symbol: { path: [3, 0, 0], type: u32 }, venue: { path: [3, 0, 1], type: u16 },
        exchange_ts: { path: [1], type: u64 }, sequence: { path: [0], type: u64 } }
    - { type: 3, admin: HEARTBEAT, sequence: { path: [0], type: u64 } }
heartbeat_timeout_ms: 50
)";

struct Rec { TickHeader hdr; std::vector<uint8_t> payload; };
std::vector<Rec> drain(ShmReader& rd) {
    std::vector<Rec> out;
    RecordView v;
    while (rd.poll(v) == ReaderStatus::Ok) {
        Rec r; r.hdr = *v.hdr; r.payload.assign(v.payload, v.payload + v.payload_len);
        REQUIRE(v.valid());
        out.push_back(std::move(r));
    }
    return out;
}

struct Harness {
    SymbolTable symbols{1024};
    Lvc lvc;
    Partition part;
    FakeTransport* fake = nullptr;
    std::unique_ptr<Pipeline> pipe;
    std::unique_ptr<Session> s;
    std::string ring_path;
    ShmReader rd;

    void build(const std::string& partition_yaml, const std::set<uint32_t>& ids) {
        std::string err;
        REQUIRE(lvc.init(1u << 20, {static_cast<uint8_t>(MsgType::TRADE), static_cast<uint8_t>(MsgType::QUOTE)}, 256, err));
        PartitionConfig cfg;
        REQUIRE_MESSAGE(PartitionConfig::parse(YAML::Load(partition_yaml), 0, "", cfg, err), err);
        auto ft = std::make_unique<FakeTransport>(8, 1500);
        fake = ft.get();
        REQUIRE_MESSAGE(part.build(cfg, err, std::move(ft)), err);
        REQUIRE(part.open(err));
        pipe = std::make_unique<Pipeline>("g0", 99, symbols, lvc);
        pipe->add_partition(&part);
        ring_path = "/dev/shm/flashtick.test.fb." + std::to_string(getpid()) + "." + std::to_string(ids.size()) + ".ring";
        s = std::make_unique<Session>();
        s->id = 3; s->name = "t"; s->owner = "config"; s->partition_mask = 1u;
        FilterSpec spec;
        spec.symbol_ids.assign(ids.begin(), ids.end());
        spec.msg_types = {static_cast<uint8_t>(MsgType::TRADE)};
        spec.partition_ids = {0};
        s->filter_owned = build_filter(spec, 1u << 20, 1, err);
        REQUIRE_MESSAGE(s->filter_owned, err);
        s->filter.store(s->filter_owned.get());
        RingConfig rc; rc.capacity_bytes = 1u << 20; rc.max_payload = 4096; rc.session_name = "t";
        REQUIRE_MESSAGE(s->ring.open(rc, ring_path, 99, err), err);
        PipelineCommand c; c.type = PipelineCommand::Type::ADD_SESSION; c.session = s.get();
        REQUIRE(pipe->post(c));
        pipe->run_once();
        REQUIRE(rd.open(ring_path));
        drain(rd); // AGENT_RESTARTED
    }
    ~Harness() {
        if (pipe && s) { PipelineCommand rm; rm.type = PipelineCommand::Type::REMOVE_SESSION; rm.session = s.get(); pipe->post(rm); pipe->run_once(); }
        if (!ring_path.empty()) RingWriter::unlink_ring(ring_path);
    }
};

} // namespace

TEST_CASE("flatbuffers partition: default framing is one buffer per packet; ring payload is the whole buffer, prefix included") {
    Harness h;
    const std::set<uint32_t> ids = {1000, 1006, 1031};
    h.build(std::string("name: fb0\ntransport: { type: udp, port: 1 }\n") + kDecoder, ids);
    CHECK(h.part.decoder_type() == DecoderType::FLATBUFFERS);
    CHECK(h.part.schema() == nullptr);
    CHECK(h.part.framer().config().len_source == LenSource::PACKET);
    CHECK(h.part.symbol_passthrough());

    auto msgs = fb_fixture_messages(); auto exp = fb_fixture_expected();
    const uint64_t t0 = now_ns();
    for (const auto& m : msgs) h.fake->push(m, t0);
    size_t want_trades = 0, want_hb = 0;
    std::vector<size_t> want_idx;
    for (size_t i = 0; i < exp.size(); ++i) {
        if (exp[i].kind == 1 && ids.count(exp[i].instrument)) { ++want_trades; want_idx.push_back(i); }
        if (exp[i].kind == 3) ++want_hb;
    }
    for (int i = 0; i < 60; ++i) h.pipe->run_once();

    auto recs = drain(h.rd);
    size_t trades = 0, hb = 0, k = 0;
    for (const auto& r : recs) {
        if (r.hdr.kind == static_cast<uint8_t>(Kind::DATA)) {
            CHECK(r.hdr.msg_type == static_cast<uint8_t>(MsgType::TRADE));
            CHECK(ids.count(r.hdr.symbol_id) == 1);
            CHECK(r.hdr.wire_format == static_cast<uint8_t>(WireFormat::FLATBUFFERS));
            CHECK(r.hdr.ingest_ts == t0);
            REQUIRE(k < want_idx.size());
            const auto& m = msgs[want_idx[k]];
            CHECK(r.payload == m);                      // bit-identical, size prefix first
            CHECK(r.hdr.sequence == exp[want_idx[k]].seq);
            CHECK(r.hdr.exchange_ts == exp[want_idx[k]].ts);
            ++k; ++trades;
        } else if (r.hdr.kind == static_cast<uint8_t>(Kind::BUS_ADMIN)) {
            CHECK(r.hdr.msg_type == static_cast<uint8_t>(ControlCode::BUS_HEARTBEAT));
            ++hb;
        }
    }
    CHECK(trades == want_trades);
    CHECK(hb == want_hb);
    auto pc = h.part.counters.snapshot();
    CHECK(pc.packets == 300);
    CHECK(pc.messages == 300);
    CHECK(pc.unknown_templates == 0);
    CHECK(pc.decode_errors == 0);
    CHECK(pc.admin == want_hb);

    TickHeader lh; std::vector<uint8_t> lp;
    REQUIRE(h.lvc.read(1006, static_cast<uint8_t>(MsgType::QUOTE), lh, lp));
    CHECK(lh.wire_format == static_cast<uint8_t>(WireFormat::FLATBUFFERS));

    // A buffer whose prefix disagrees with the packet is a decode error, never routed.
    auto bad = msgs[1]; bad.push_back(0);
    h.fake->push(bad, now_ns());
    h.pipe->run_once();
    CHECK(h.part.counters.snapshot().decode_errors == 1);
    CHECK(drain(h.rd).empty());
}

TEST_CASE("flatbuffers partition: several size-prefixed buffers per packet with message_prefix + keep_prefix") {
    Harness h;
    const std::set<uint32_t> ids = {1000, 1006, 1031, 1040};
    h.build(std::string("name: fb1\ntransport: { type: udp, port: 1 }\n"
                        "framing: { message_prefix: [ { name: size, type: u32le } ], length: message_prefix.size, keep_prefix: true }\n") + kDecoder, ids);
    auto msgs = fb_fixture_messages(); auto exp = fb_fixture_expected();
    size_t want = 0;
    for (const auto& e : exp) if (e.kind == 1 && ids.count(e.instrument)) ++want;
    size_t i = 0; int packets = 0;
    while (i < msgs.size()) {
        std::vector<uint8_t> pkt;
        for (int k = 0; k < 4 && i < msgs.size(); ++k, ++i) pkt.insert(pkt.end(), msgs[i].begin(), msgs[i].end());
        h.fake->push(pkt, now_ns());
        ++packets;
    }
    for (int r = 0; r < 30; ++r) h.pipe->run_once();
    auto recs = drain(h.rd);
    size_t trades = 0;
    for (const auto& r : recs) if (r.hdr.kind == static_cast<uint8_t>(Kind::DATA)) { CHECK(r.payload.size() >= 8); CHECK(ids.count(r.hdr.symbol_id) == 1); ++trades; }
    CHECK(trades == want);
    auto pc = h.part.counters.snapshot();
    CHECK(pc.packets == static_cast<uint64_t>(packets));
    CHECK(pc.messages == 300);
    CHECK(pc.decode_errors == 0);
}

TEST_CASE("flatbuffers partition: length: schema is rejected, unknown decoder lists all three") {
    std::string err;
    PartitionConfig cfg;
    Partition p1;
    REQUIRE(PartitionConfig::parse(YAML::Load(std::string("name: bad\ntransport: { type: udp, port: 1 }\nframing: { length: schema }\n") + kDecoder), 0, "", cfg, err));
    CHECK_FALSE(p1.build(cfg, err, std::make_unique<FakeTransport>()));
    CHECK(err.find("length: packet") != std::string::npos);
    Partition p2;
    REQUIRE(PartitionConfig::parse(YAML::Load("name: unk\ntransport: { type: udp, port: 1 }\ndecoder: { type: protobuf }"), 0, "", cfg, err));
    CHECK_FALSE(p2.build(cfg, err, std::make_unique<FakeTransport>()));
    CHECK(err.find("available: sbe_routing dbn flatbuffers") != std::string::npos);
}
