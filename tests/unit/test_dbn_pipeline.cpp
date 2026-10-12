// DBN records through the real pipeline with a fake transport: proves the
// "encoding is per-partition configuration" rule end to end (decoder dbn,
// framing length: packet, sessions filtered by instrument id and msg type).

#include <doctest.h>

#include "fake_transport.h"
#include "flashtick/client/shm_reader.h"
#include "flashtick/core/clock.h"
#include "flashtick/pipeline/pipeline.h"
#include "flashtick/shm/control_events.h"

#include <cstring>
#include <string>
#include <unistd.h>
#include <vector>

using namespace flashtick;
using flashtick::test::FakeTransport;

namespace {

std::vector<uint8_t> rec(uint32_t total, uint8_t rtype, uint32_t iid, uint64_t ts, uint16_t seq_off = 0, uint32_t seq = 0) {
    std::vector<uint8_t> b(total, 0);
    b[0] = static_cast<uint8_t>(total / 4); b[1] = rtype; b[2] = 1; b[3] = 0;
    std::memcpy(b.data() + 4, &iid, 4);
    std::memcpy(b.data() + 8, &ts, 8);
    if (seq_off) std::memcpy(b.data() + seq_off, &seq, 4);
    return b;
}

const char* kYaml = R"(
name: db0
transport: { type: tcp_stream, host: 127.0.0.1, port: 1 }
decoder: { type: dbn }
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

} // namespace

TEST_CASE("dbn partition: default framing is length: packet, no schema, records route by instrument and type") {
    SymbolTable symbols{1024};
    Lvc lvc;
    std::string err;
    REQUIRE(lvc.init(1u << 20, {static_cast<uint8_t>(MsgType::TRADE), static_cast<uint8_t>(MsgType::BOOK_UPDATE)}, 256, err));
    Partition part;
    PartitionConfig cfg;
    REQUIRE_MESSAGE(PartitionConfig::parse(YAML::Load(kYaml), 0, "", cfg, err), err);
    auto ft = std::make_unique<FakeTransport>(8, 1024);
    FakeTransport* fake = ft.get();
    REQUIRE_MESSAGE(part.build(cfg, err, std::move(ft)), err);
    REQUIRE(part.open(err));
    CHECK(part.decoder_type() == DecoderType::DBN);
    CHECK(part.schema() == nullptr);
    CHECK(part.framer().config().len_source == LenSource::PACKET);
    CHECK(part.framer().seq_source() == SeqSource::NONE);

    Pipeline pipe("g0", 99, symbols, lvc);
    pipe.add_partition(&part);

    // Session: instrument 432669 only, trades + book updates, bus admin on.
    const std::string ring_path = "/dev/shm/flashtick.test.dbn." + std::to_string(getpid()) + ".ring";
    auto s = std::make_unique<Session>();
    s->id = 3; s->name = "t"; s->owner = "config"; s->partition_mask = 1u;
    FilterSpec spec;
    spec.symbol_ids = {432669};
    spec.msg_types = {static_cast<uint8_t>(MsgType::TRADE), static_cast<uint8_t>(MsgType::BOOK_UPDATE)};
    spec.partition_ids = {0};
    s->filter_owned = build_filter(spec, 1u << 20, 1, err);
    REQUIRE_MESSAGE(s->filter_owned, err);
    s->filter.store(s->filter_owned.get());
    RingConfig rc; rc.capacity_bytes = 1u << 20; rc.max_payload = 4096; rc.session_name = "t";
    REQUIRE_MESSAGE(s->ring.open(rc, ring_path, 99, err), err);
    PipelineCommand c; c.type = PipelineCommand::Type::ADD_SESSION; c.session = s.get();
    REQUIRE(pipe.post(c));
    pipe.run_once();

    ShmReader rd;
    REQUIRE(rd.open(ring_path));
    drain(rd); // AGENT_RESTARTED

    const uint64_t t0 = now_ns();
    fake->push(rec(56, 0xA0, 432669, 1758742200000860311ull, 52, 10), t0); // MBO -> BOOK_UPDATE, kept
    fake->push(rec(48, 0x00, 432669, 2, 44, 11), t0);                       // trade, kept
    fake->push(rec(56, 0xA0, 555555, 3, 52, 12), t0);                       // other instrument: filtered
    fake->push(rec(80, 0x01, 432669, 4, 44, 13), t0);                       // MBP-1 -> QUOTE: filtered by type
    fake->push(rec(320, 0x17, 0, 5), t0);                                   // SYSTEM -> BUS_HEARTBEAT: delivered (bus_admin)
    fake->push(rec(520, 0x13, 432669, 6), t0);                              // DEFINITION: unmapped -> unknown_templates
    fake->push(rec(56, 0xA0, 432669, 7, 52, 14), t0);                       // kept
    pipe.run_once();

    auto recs = drain(rd);
    REQUIRE(recs.size() == 4);
    CHECK(recs[0].hdr.msg_type == static_cast<uint8_t>(MsgType::BOOK_UPDATE));
    CHECK(recs[0].hdr.symbol_id == 432669);
    CHECK(recs[0].hdr.sequence == 10);
    CHECK(recs[0].hdr.exchange_ts == 1758742200000860311ull);
    CHECK(recs[0].hdr.wire_format == static_cast<uint8_t>(WireFormat::DBN));
    CHECK(recs[0].hdr.template_id == 0xA0);
    CHECK(recs[0].hdr.ingest_ts == t0);
    CHECK(recs[0].payload.size() == 56);
    CHECK(recs[0].payload[0] == 14);                 // bit-identical DBN bytes
    CHECK(recs[1].hdr.msg_type == static_cast<uint8_t>(MsgType::TRADE));
    CHECK(recs[1].hdr.sequence == 11);
    CHECK(recs[2].hdr.kind == static_cast<uint8_t>(Kind::BUS_ADMIN));
    CHECK(recs[2].hdr.msg_type == static_cast<uint8_t>(ControlCode::BUS_HEARTBEAT));
    CHECK(recs[3].hdr.sequence == 14);

    auto pc = part.counters.snapshot();
    CHECK(pc.packets == 7);
    CHECK(pc.messages == 6);
    CHECK(pc.unknown_templates == 1);
    CHECK(part.counters.last_unknown_template.load() == 0x13);
    CHECK(pc.admin == 1);
    CHECK(pc.decode_errors == 0);
    auto sc = s->counters.snapshot();
    CHECK(sc.filtered == 2);

    // LVC keyed by the passthrough instrument id.
    TickHeader lh; std::vector<uint8_t> lp;
    REQUIRE(lvc.read(432669, static_cast<uint8_t>(MsgType::BOOK_UPDATE), lh, lp));
    CHECK(lh.sequence == 14);

    // A misaligned record (length byte disagrees) is a decode error, never routed.
    auto bad = rec(56, 0xA0, 432669, 8, 52, 15);
    bad.push_back(0);
    fake->push(bad, now_ns());
    pipe.run_once();
    CHECK(part.counters.snapshot().decode_errors == 1);
    CHECK(drain(rd).empty());

    PipelineCommand rm; rm.type = PipelineCommand::Type::REMOVE_SESSION; rm.session = s.get();
    pipe.post(rm); pipe.run_once();
    RingWriter::unlink_ring(ring_path);
}

TEST_CASE("dbn partition: length: schema is rejected, sequence: message is accepted") {
    std::string err;
    PartitionConfig cfg;
    Partition p1;
    REQUIRE(PartitionConfig::parse(YAML::Load(R"(
name: bad
transport: { type: tcp_stream, host: 127.0.0.1, port: 1 }
framing: { length: schema }
decoder: { type: dbn })"), 0, "", cfg, err));
    CHECK_FALSE(p1.build(cfg, err, std::make_unique<FakeTransport>()));
    CHECK(err.find("length: packet") != std::string::npos);

    Partition p2;
    REQUIRE(PartitionConfig::parse(YAML::Load(R"(
name: ok
transport: { type: tcp_stream, host: 127.0.0.1, port: 1 }
framing: { length: packet, sequence: message }
decoder: { type: dbn })"), 0, "", cfg, err));
    REQUIRE_MESSAGE(p2.build(cfg, err, std::make_unique<FakeTransport>()), err);
    CHECK(p2.framer().seq_source() == SeqSource::MESSAGE_FIELD);

    Partition p3;
    REQUIRE(PartitionConfig::parse(YAML::Load(R"(
name: unk
transport: { type: tcp_stream, host: 127.0.0.1, port: 1 }
decoder: { type: protobuf })"), 0, "", cfg, err));
    CHECK_FALSE(p3.build(cfg, err, std::make_unique<FakeTransport>()));
    CHECK(err.find("available: sbe_routing dbn") != std::string::npos);
}
