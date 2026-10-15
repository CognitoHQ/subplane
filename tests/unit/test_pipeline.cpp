#include <doctest.h>

#include "fake_transport.h"
#include "subplane/client/shm_reader.h"
#include "subplane/core/clock.h"
#include "subplane/pipeline/pipeline.h"
#include "subplane/shm/control_events.h"

#include <cstring>
#include <string>
#include <unistd.h>
#include <vector>

using namespace subplane;
using subplane::test::FakeTransport;

namespace {

std::string data_dir() {
    std::string f = __FILE__;
    return f.substr(0, f.find_last_of('/')) + "/../data";
}

void put_u16le(std::vector<uint8_t>& b, uint16_t v) { b.push_back(static_cast<uint8_t>(v)); b.push_back(static_cast<uint8_t>(v >> 8)); }

// Builds an SBE message for the sample schema from (name, value) pairs on root fields.
struct Enc {
    const SbeSchema& s;
    std::vector<uint8_t> msg(uint16_t tid, std::initializer_list<std::pair<const char*, uint64_t>> vals) const {
        const MessageDef* m = s.find_message(tid);
        REQUIRE(m);
        std::vector<uint8_t> b;
        put_u16le(b, m->block_length);
        put_u16le(b, tid);
        put_u16le(b, s.schema_id());
        put_u16le(b, s.version());
        std::vector<uint8_t> root(m->block_length, 0);
        for (auto& [name, v] : vals) {
            const FieldDef* f = s.find_field(*m, name);
            REQUIRE_MESSAGE(f, name);
            for (uint16_t i = 0; i < f->size && i < 8; ++i) root[f->offset + i] = static_cast<uint8_t>(v >> (8 * i));
        }
        b.insert(b.end(), root.begin(), root.end());
        return b;
    }
};

std::vector<uint8_t> packet(const std::vector<std::vector<uint8_t>>& msgs) {
    std::vector<uint8_t> p;
    for (auto& m : msgs) p.insert(p.end(), m.begin(), m.end());
    return p;
}

const char* kPartitionYaml = R"(
name: p0
transport: { type: udp_multicast, group: "", port: 1 }
framing: { sequence: message.SeqNum }
decoder:
  type: sbe_routing
  schema: sample_md.xml
  fields: { symbol: SecurityID, sequence: SeqNum, exchange_ts: TransactTime, venue: MarketSegmentID }
  symbol_id_mode: table
  messages:
    - { template: 1,   msg_type: TRADE }
    - { template: 2,   msg_type: QUOTE }
    - { template: 100, admin: HEARTBEAT }
    - { template: 101, admin: SEQ_RESET }
heartbeat_timeout_ms: 50
)";

struct Harness {
    SymbolTable symbols{1024};
    Lvc lvc;
    Partition part;
    FakeTransport* fake = nullptr;
    Pipeline pipe;
    std::string ring_path;

    explicit Harness(const char* yaml = kPartitionYaml) : pipe("g0", 4242, symbols, lvc) {
        std::string err;
        REQUIRE(lvc.init(1024, {static_cast<uint8_t>(MsgType::TRADE), static_cast<uint8_t>(MsgType::QUOTE)}, 256, err));
        PartitionConfig cfg;
        REQUIRE_MESSAGE(PartitionConfig::parse(YAML::Load(yaml), 0, data_dir(), cfg, err), err);
        auto ft = std::make_unique<FakeTransport>();
        fake = ft.get();
        REQUIRE_MESSAGE(part.build(cfg, err, std::move(ft)), err);
        REQUIRE(part.open(err));
        pipe.add_partition(&part);
        ring_path = "/dev/shm/subplane.test." + std::to_string(getpid()) + "." + std::to_string(reinterpret_cast<uintptr_t>(this) & 0xffff) + ".ring";
    }
    ~Harness() { RingWriter::unlink_ring(ring_path); }

    std::unique_ptr<Session> make_session(const FilterSpec& spec, uint32_t gen = 1) {
        auto s = std::make_unique<Session>();
        s->id = 7;
        s->name = "t";
        s->owner = "config";
        s->partition_mask = 1u;
        std::string err;
        s->filter_owned = build_filter(spec, 1024, gen, err);
        REQUIRE_MESSAGE(s->filter_owned, err);
        s->filter.store(s->filter_owned.get());
        RingConfig rc;
        rc.capacity_bytes = 1u << 20;
        rc.max_payload = 4096;
        rc.session_name = "t";
        REQUIRE_MESSAGE(s->ring.open(rc, ring_path, 4242, err), err);
        return s;
    }

    void add(Session* s) {
        PipelineCommand c;
        c.type = PipelineCommand::Type::ADD_SESSION;
        c.session = s;
        REQUIRE(pipe.post(c));
        pipe.run_once();
        REQUIRE(pipe.commands_applied() == pipe.commands_posted());
    }

    const SbeSchema& schema() const { return *part.schema(); }
};

struct Rec { TickHeader hdr; std::vector<uint8_t> payload; };

std::vector<Rec> drain(ShmReader& rd) {
    std::vector<Rec> out;
    RecordView v;
    while (rd.poll(v) == ReaderStatus::Ok) {
        Rec r;
        r.hdr = *v.hdr;
        r.payload.assign(v.payload, v.payload + v.payload_len);
        REQUIRE(v.valid());
        out.push_back(std::move(r));
    }
    return out;
}

} // namespace

TEST_CASE("pipeline: data flows through filter to ring with routing fields and in-band events") {
    Harness h;
    FilterSpec spec;
    spec.all_symbols = true;
    spec.msg_types = {static_cast<uint8_t>(MsgType::TRADE)};
    spec.partition_ids = {0};
    auto s = h.make_session(spec);
    ShmReader rd;
    REQUIRE(rd.open(h.ring_path));
    h.add(s.get());

    auto recs = drain(rd);
    REQUIRE(recs.size() == 1);
    CHECK(recs[0].hdr.msg_type == static_cast<uint8_t>(ControlCode::AGENT_RESTARTED));
    AgentRestartedEvent ar{};
    std::memcpy(&ar, recs[0].payload.data(), sizeof(ar));
    CHECK(ar.epoch == 4242);

    Enc e{h.schema()};
    auto t1 = e.msg(1, {{"SecurityID", 1001}, {"SeqNum", 1}, {"TransactTime", 555}, {"MarketSegmentID", 3}});
    auto q1 = e.msg(2, {{"SecurityID", 1001}, {"SeqNum", 2}, {"TransactTime", 556}, {"MarketSegmentID", 3}});
    auto t2 = e.msg(1, {{"SecurityID", 1002}, {"SeqNum", 3}, {"TransactTime", 557}, {"MarketSegmentID", 3}});
    const uint64_t t0 = now_ns();
    h.fake->push(packet({t1, q1, t2}), t0);
    h.pipe.run_once();

    recs = drain(rd);
    // SYMBOL_MAPPED for 1001, trade 1001, (quote filtered), SYMBOL_MAPPED for 1002, trade 1002
    REQUIRE(recs.size() == 4);
    CHECK(recs[0].hdr.msg_type == static_cast<uint8_t>(ControlCode::SYMBOL_MAPPED));
    CHECK(recs[1].hdr.kind == static_cast<uint8_t>(Kind::DATA));
    CHECK(recs[1].hdr.msg_type == static_cast<uint8_t>(MsgType::TRADE));
    CHECK(recs[1].hdr.symbol_id == 1);
    CHECK(recs[1].hdr.sequence == 1);
    CHECK(recs[1].hdr.exchange_ts == 555);
    CHECK(recs[1].hdr.venue_id == 3);
    CHECK(recs[1].hdr.ingest_ts == t0);
    CHECK(recs[1].hdr.partition_id == 0);
    CHECK(recs[1].hdr.template_id == 1);
    CHECK(recs[1].hdr.wire_format == static_cast<uint8_t>(WireFormat::SBE));
    CHECK(recs[1].payload == t1); // bit-identical bus bytes
    CHECK(recs[2].hdr.msg_type == static_cast<uint8_t>(ControlCode::SYMBOL_MAPPED));
    CHECK(recs[3].hdr.symbol_id == 2);
    CHECK(recs[3].hdr.sequence == 3);

    auto pc = h.part.counters.snapshot();
    CHECK(pc.packets == 1);
    CHECK(pc.messages == 3);
    auto sc = s->counters.snapshot();
    CHECK(sc.published >= 2);
    CHECK(sc.filtered == 1);

    // LVC has the latest trade for symbol 2 and the quote for symbol 1
    TickHeader lh;
    std::vector<uint8_t> lp;
    REQUIRE(h.lvc.read(2, static_cast<uint8_t>(MsgType::TRADE), lh, lp));
    CHECK(lh.sequence == 3);
    CHECK(lp == t2);
    REQUIRE(h.lvc.read(1, static_cast<uint8_t>(MsgType::QUOTE), lh, lp));
    CHECK(lh.sequence == 2);
    CHECK_FALSE(h.lvc.read(3, static_cast<uint8_t>(MsgType::TRADE), lh, lp));

    // Symbol names resolvable
    std::string name;
    REQUIRE(h.symbols.resolve(1, name));
    CHECK(name == "1001");

    // histogram got one packet
    CHECK(h.pipe.hist_recv_to_publish().snapshot().count == 1);
}

TEST_CASE("pipeline: sequence gaps, duplicates, seq reset, heartbeat admin, unknown template") {
    Harness h;
    FilterSpec spec;
    spec.all_symbols = true;
    spec.msg_types = {static_cast<uint8_t>(MsgType::TRADE), static_cast<uint8_t>(MsgType::QUOTE)};
    spec.partition_ids = {0};
    auto s = h.make_session(spec);
    ShmReader rd;
    REQUIRE(rd.open(h.ring_path));
    h.add(s.get());
    drain(rd);

    Enc e{h.schema()};
    auto m = [&](uint64_t seq) { return e.msg(1, {{"SecurityID", 5}, {"SeqNum", seq}, {"TransactTime", 1}}); };
    h.fake->push(packet({m(10)}), now_ns());
    h.fake->push(packet({m(11)}), now_ns());
    h.fake->push(packet({m(13)}), now_ns());  // gap: 12 missing
    h.fake->push(packet({m(13)}), now_ns());  // duplicate
    h.fake->push(packet({e.msg(100, {{"SeqNum", 14}})}), now_ns()); // heartbeat admin
    h.fake->push(packet({e.msg(101, {{"SeqNum", 15}, {"NewSeq", 1}})}), now_ns()); // seq reset
    h.fake->push(packet({m(1)}), now_ns());   // new stream after reset: no gap
    {
        std::vector<uint8_t> unk;
        put_u16le(unk, 4); put_u16le(unk, 999); put_u16le(unk, 7); put_u16le(unk, 2); put_u16le(unk, 0); put_u16le(unk, 0);
        h.fake->push(packet({m(2), unk}), now_ns()); // unknown template after a good message -> undecodable tail
    }
    while (h.fake->pending()) h.pipe.run_once();

    auto recs = drain(rd);
    std::vector<uint8_t> kinds;
    for (auto& r : recs) kinds.push_back(r.hdr.msg_type);
    // SYMBOL_MAPPED(5), T10, T11, GAP, T13, HEARTBEAT, SEQ_RESET, T1, T2
    REQUIRE(recs.size() == 9);
    CHECK(recs[0].hdr.msg_type == static_cast<uint8_t>(ControlCode::SYMBOL_MAPPED));
    CHECK(recs[1].hdr.sequence == 10);
    CHECK(recs[2].hdr.sequence == 11);
    CHECK(recs[3].hdr.msg_type == static_cast<uint8_t>(ControlCode::GAP_DETECTED));
    GapEvent g{};
    std::memcpy(&g, recs[3].payload.data(), sizeof(g));
    CHECK(g.expected_seq == 12);
    CHECK(g.received_seq == 13);
    CHECK(g.partition_id == 0);
    CHECK(recs[4].hdr.sequence == 13);
    CHECK(recs[5].hdr.kind == static_cast<uint8_t>(Kind::BUS_ADMIN));
    CHECK(recs[5].hdr.msg_type == static_cast<uint8_t>(ControlCode::BUS_HEARTBEAT));
    CHECK(recs[6].hdr.msg_type == static_cast<uint8_t>(ControlCode::BUS_SEQ_RESET));
    CHECK(recs[7].hdr.sequence == 1);
    CHECK(recs[8].hdr.sequence == 2);

    auto pc = h.part.counters.snapshot();
    CHECK(pc.gaps == 1);
    CHECK(pc.dups == 1);
    CHECK(pc.admin == 2);
    CHECK(pc.undecodable_tail == 1);
    CHECK(pc.packets == 8);

    // Publisher restart without SEQ_RESET: jump from 2 back to 1 is within dup_window -> dup;
    // a jump from 5000 back to 1 is beyond it -> treated as a reset with an in-band GAP.
    h.fake->push(packet({m(5000)}), now_ns());
    h.fake->push(packet({m(1)}), now_ns());
    h.fake->push(packet({m(2)}), now_ns());
    while (h.fake->pending()) h.pipe.run_once();
    recs = drain(rd);
    REQUIRE(recs.size() == 5); // GAP(3->5000), T5000, GAP(reset), T1, T2
    CHECK(recs[2].hdr.msg_type == static_cast<uint8_t>(ControlCode::GAP_DETECTED));
    std::memcpy(&g, recs[2].payload.data(), sizeof(g));
    CHECK(g.expected_seq == 5001);
    CHECK(g.received_seq == 1);
    CHECK(recs[3].hdr.sequence == 1);
    CHECK(recs[4].hdr.sequence == 2);
    CHECK(h.part.counters.snapshot().seq_resets == 1);
}

TEST_CASE("pipeline: filter swap and session removal emit in-band events; partition degrades and recovers") {
    Harness h;
    FilterSpec spec;
    spec.all_symbols = true;
    spec.msg_types = {static_cast<uint8_t>(MsgType::TRADE)};
    spec.partition_ids = {0};
    auto s = h.make_session(spec, 1);
    ShmReader rd;
    REQUIRE(rd.open(h.ring_path));
    h.add(s.get());
    drain(rd);

    Enc e{h.schema()};
    h.fake->push(packet({e.msg(1, {{"SecurityID", 9}, {"SeqNum", 1}})}), now_ns());
    h.pipe.run_once();
    drain(rd);
    CHECK(h.part.state == PartitionState::ACTIVE);

    // Swap to a filter that only accepts QUOTE
    FilterSpec spec2 = spec;
    spec2.msg_types = {static_cast<uint8_t>(MsgType::QUOTE)};
    std::string err;
    auto f2 = build_filter(spec2, 1024, 2, err);
    REQUIRE(f2);
    PipelineCommand c;
    c.type = PipelineCommand::Type::SWAP_FILTER;
    c.session = s.get();
    c.filter = f2.get();
    c.generation = 2;
    REQUIRE(h.pipe.post(c));
    h.pipe.run_once();
    CHECK(s->applied_generation.load() == 2);
    auto recs = drain(rd);
    REQUIRE(recs.size() == 1);
    CHECK(recs[0].hdr.msg_type == static_cast<uint8_t>(ControlCode::FILTER_RELOADED));

    h.fake->push(packet({e.msg(1, {{"SecurityID", 9}, {"SeqNum", 2}}), e.msg(2, {{"SecurityID", 9}, {"SeqNum", 3}})}), now_ns() - 100'000'000ull); // old timestamp: will trip the 50ms heartbeat timeout
    h.pipe.run_once();
    recs = drain(rd);
    REQUIRE(recs.size() >= 1);
    CHECK(recs[0].hdr.msg_type == static_cast<uint8_t>(MsgType::QUOTE));

    // The previous packet carried an old receive timestamp, so service_timeouts in the same
    // iteration already degraded the partition and emitted the event after the QUOTE.
    CHECK(h.part.state == PartitionState::DEGRADED);
    REQUIRE(recs.size() == 2);
    CHECK(recs[1].hdr.msg_type == static_cast<uint8_t>(ControlCode::PARTITION_DEGRADED));
    // Recover on the next packet
    h.fake->push(packet({e.msg(2, {{"SecurityID", 9}, {"SeqNum", 4}})}), now_ns());
    h.pipe.run_once();
    recs = drain(rd);
    REQUIRE(recs.size() == 2);
    CHECK(recs[0].hdr.msg_type == static_cast<uint8_t>(ControlCode::PARTITION_RECOVERED));
    CHECK(recs[1].hdr.sequence == 4);
    CHECK(h.part.counters.snapshot().degraded_events == 1);

    // Remove session
    c = PipelineCommand{};
    c.type = PipelineCommand::Type::REMOVE_SESSION;
    c.session = s.get();
    c.generation = 1;
    REQUIRE(h.pipe.post(c));
    h.pipe.run_once();
    recs = drain(rd);
    REQUIRE(recs.size() == 1);
    CHECK(recs[0].hdr.msg_type == static_cast<uint8_t>(ControlCode::SESSION_CLOSING));
    CHECK_FALSE(s->attached.load());
    CHECK(h.part.n_sessions == 0);
}

TEST_CASE("pipeline: threaded start/stop with pinned cpu off and commands applied") {
    Harness h;
    FilterSpec spec;
    spec.all_symbols = true;
    spec.partition_ids = {0};
    spec.msg_types = {static_cast<uint8_t>(MsgType::TRADE)};
    auto s = h.make_session(spec);
    std::string err;
    REQUIRE(h.pipe.start(-1, err));
    PipelineCommand c;
    c.type = PipelineCommand::Type::ADD_SESSION;
    c.session = s.get();
    REQUIRE(h.pipe.post(c));
    CHECK(h.pipe.wait_applied(2'000'000'000ull));
    CHECK(s->attached.load());
    h.pipe.stop();
    CHECK_FALSE(h.pipe.running());
    CHECK(h.pipe.loops() > 0);
}

TEST_CASE("pipeline: transport down and up reach the partition's sessions in order, ahead of the batch") {
    Harness h;
    FilterSpec spec;
    spec.all_symbols = true;
    spec.msg_types = {static_cast<uint8_t>(MsgType::TRADE)};
    spec.partition_ids = {0};
    auto s = h.make_session(spec, 1);
    ShmReader rd;
    REQUIRE(rd.open(h.ring_path));
    h.add(s.get());
    drain(rd);

    auto event = [](const Rec& r) {
        TransportStateEvent ev{};
        REQUIRE(r.payload.size() == sizeof(ev));
        std::memcpy(&ev, r.payload.data(), sizeof(ev));
        return ev;
    };
    const auto DOWN = static_cast<uint8_t>(ControlCode::TRANSPORT_DOWN);
    const auto UP = static_cast<uint8_t>(ControlCode::TRANSPORT_UP);
    Enc e{h.schema()};

    // First contact: UP precedes the records of the batch that brought it.
    h.fake->up();
    h.fake->push(packet({e.msg(1, {{"SecurityID", 9}, {"SeqNum", 1}})}), now_ns());
    h.pipe.run_once();
    auto recs = drain(rd);
    REQUIRE(recs.size() == 3); // UP, SYMBOL_MAPPED, trade
    CHECK(recs[0].hdr.msg_type == UP);
    CHECK(recs[0].hdr.kind == static_cast<uint8_t>(Kind::AGENT_CONTROL));
    CHECK(recs[0].hdr.partition_id == 0);
    CHECK(event(recs[0]).reason == static_cast<uint32_t>(TransportStateReason::NONE));
    CHECK(recs[1].hdr.msg_type == static_cast<uint8_t>(ControlCode::SYMBOL_MAPPED));
    CHECK(recs[2].hdr.kind == static_cast<uint8_t>(Kind::DATA));
    CHECK(recs[2].hdr.sequence == 1);

    // Source lost, reported twice: one event, carrying the last sequence seen.
    h.fake->down(TransportReason::SOURCE_LOST);
    h.fake->down(TransportReason::LOCAL_FAILURE);
    h.pipe.run_once();
    recs = drain(rd);
    REQUIRE(recs.size() == 1);
    CHECK(recs[0].hdr.msg_type == DOWN);
    CHECK(event(recs[0]).reason == static_cast<uint32_t>(TransportStateReason::SOURCE_LOST));
    CHECK(event(recs[0]).last_seq == 1);
    CHECK(event(recs[0]).changed_ns != 0);
    CHECK(h.part.transport().state() == TransportState::DOWN);

    // Back, then a new publisher instance while up: the restart is reported even without a DOWN.
    h.fake->up();
    h.fake->up();
    h.pipe.run_once();
    h.fake->up(TransportReason::PUBLISHER_RESTART);
    h.pipe.run_once();
    recs = drain(rd);
    REQUIRE(recs.size() == 2);
    CHECK(recs[0].hdr.msg_type == UP);
    CHECK(event(recs[0]).reason == static_cast<uint32_t>(TransportStateReason::NONE));
    CHECK(recs[1].hdr.msg_type == UP);
    CHECK(event(recs[1]).reason == static_cast<uint32_t>(TransportStateReason::PUBLISHER_RESTART));

    const auto c = h.part.counters.snapshot();
    CHECK(c.transport_down == 1);
    CHECK(c.transport_up == 3);
    CHECK(h.part.transport().state() == TransportState::UP);
    CHECK(h.part.transport().state_change_count() == 0);
}

TEST_CASE("pipeline: a session without agent_control does not see transport events") {
    Harness h;
    FilterSpec spec;
    spec.all_symbols = true;
    spec.partition_ids = {0};
    spec.agent_control = false;
    auto s = h.make_session(spec, 1);
    ShmReader rd;
    REQUIRE(rd.open(h.ring_path));
    h.add(s.get());
    drain(rd);
    h.fake->down(TransportReason::LOCAL_FAILURE);
    h.pipe.run_once();
    CHECK(drain(rd).empty());
    CHECK(h.part.counters.snapshot().transport_down == 1);
}

TEST_CASE("pipeline: a recap marker on the live stream is counted and dropped; lvc: off skips the cache") {
    const std::string yaml = std::string(kPartitionYaml) + R"(lvc: off
)";
    std::string marker_yaml = yaml;
    const std::string from = "    - { template: 101, admin: SEQ_RESET }";
    marker_yaml.replace(marker_yaml.find(from), from.size(),
                        "    - { template: 101, admin: RECAP_BEGIN, fields: { request_id: SeqNum, as_of: NewSeq } }");
    Harness h(marker_yaml.c_str());
    CHECK_FALSE(h.part.lvc_enabled);
    FilterSpec spec;
    spec.all_symbols = true;
    spec.msg_types = {static_cast<uint8_t>(MsgType::TRADE)};
    spec.partition_ids = {0};
    auto s = h.make_session(spec, 1);
    ShmReader rd;
    REQUIRE(rd.open(h.ring_path));
    h.add(s.get());
    drain(rd);

    Enc e{h.schema()};
    h.fake->push(packet({e.msg(1, {{"SecurityID", 9}, {"SeqNum", 1}}),
                         e.msg(101, {{"SeqNum", 77}, {"NewSeq", 5000}}),
                         e.msg(1, {{"SecurityID", 9}, {"SeqNum", 2}})}), now_ns());
    h.pipe.run_once();
    auto recs = drain(rd);
    REQUIRE(recs.size() == 3); // SYMBOL_MAPPED, trade 1, trade 2: no marker, no gap
    CHECK(recs[1].hdr.sequence == 1);
    CHECK(recs[2].hdr.sequence == 2);
    const auto c = h.part.counters.snapshot();
    CHECK(c.recap_markers == 1);
    CHECK(c.gaps == 0);
    CHECK(c.admin == 0);

    TickHeader lh{};
    std::vector<uint8_t> lp;
    CHECK_FALSE(h.lvc.read(1, static_cast<uint8_t>(MsgType::TRADE), lh, lp));
}

TEST_CASE("partition config: lvc must be on or off") {
    PartitionConfig cfg;
    std::string err;
    REQUIRE_MESSAGE(PartitionConfig::parse(YAML::Load(kPartitionYaml), 0, data_dir(), cfg, err), err);
    CHECK(cfg.lvc);
    CHECK_FALSE(PartitionConfig::parse(YAML::Load(std::string(kPartitionYaml) + "lvc: sometimes\n"), 0, data_dir(), cfg, err));
    CHECK(err.find("lvc must be on or off") != std::string::npos);
    REQUIRE(PartitionConfig::parse(YAML::Load(std::string(kPartitionYaml) + "lvc: on\n"), 0, data_dir(), cfg, err));
    CHECK(cfg.lvc);
}
