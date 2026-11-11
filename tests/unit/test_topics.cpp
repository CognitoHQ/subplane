#include <doctest.h>

#include "fake_transport.h"
#include "subplane/client/shm_reader.h"
#include "subplane/core/clock.h"
#include "subplane/pipeline/pipeline.h"
#include "subplane/pipeline/topic_registry.h"
#include "subplane/shm/control_events.h"

#include <cstring>
#include <string>
#include <unistd.h>
#include <vector>

using namespace subplane;
using subplane::test::FakeTransport;

namespace {

std::string repo_dir() {
    std::string f = __FILE__;
    return f.substr(0, f.find_last_of('/')) + "/../..";
}

TopicRegistry registry(const char* yaml) {
    TopicRegistry r;
    std::string err;
    REQUIRE_MESSAGE(r.load(YAML::Load(yaml), "test", err), err);
    return r;
}

} // namespace

TEST_CASE("topic names, patterns and stream ids") {
    std::string err;
    CHECK(valid_topic_name("l1.crypto.binance-spot", err));
    CHECK(valid_topic_name("eq_us", err));
    CHECK_FALSE(valid_topic_name("", err));
    CHECK_FALSE(valid_topic_name("l1:crypto", err));   // not Kafka-legal
    CHECK_FALSE(valid_topic_name("a..b", err));
    CHECK_FALSE(valid_topic_name(".a", err));
    CHECK_FALSE(valid_topic_name(std::string(49, 'a'), err));
    CHECK(valid_topic_name(std::string(48, 'a'), err));

    CHECK(topic_matches("crypto.binance", "crypto.binance"));
    CHECK_FALSE(topic_matches("crypto.binance", "crypto.binance.x"));
    CHECK(topic_matches("crypto.*", "crypto.okx"));
    CHECK_FALSE(topic_matches("crypto.*", "crypto"));
    CHECK_FALSE(topic_matches("crypto.*", "crypto.okx.spot"));
    CHECK(topic_matches("crypto.>", "crypto.okx"));
    CHECK(topic_matches("crypto.>", "crypto.okx.spot"));
    CHECK_FALSE(topic_matches("crypto.>", "crypto"));
    CHECK(topic_matches("*.okx", "crypto.okx"));
    CHECK(topic_matches(">", "anything.at.all"));

    CHECK(valid_topic_pattern("crypto.*", err));
    CHECK(valid_topic_pattern("*.>", err));
    CHECK_FALSE(valid_topic_pattern("crypto.>.x", err));
    CHECK_FALSE(valid_topic_pattern("cry*", err));
    CHECK_FALSE(valid_topic_pattern("a..b", err));

    static_assert(stream_id_of(11, 1) == 720897);
    static_assert(stream_id_of(32767, 65535) == 0x7FFFFFFF);   // the largest is still a positive int32
}

TEST_CASE("topic registry rejects bad entries") {
    TopicRegistry r;
    std::string err;
    CHECK(r.load(YAML::Load("[ { id: 1, name: a }, { id: 32767, name: b.c } ]"), "t", err));
    REQUIRE(r.find("b.c"));
    CHECK(r.find("b.c")->id == 32767);
    CHECK(r.find(uint16_t{1})->name == "a");
    CHECK(r.find("x") == nullptr);
    CHECK_FALSE(r.load(YAML::Load("[ { id: 0, name: a } ]"), "t", err));
    CHECK(err.find("reserved") != std::string::npos);
    CHECK_FALSE(r.load(YAML::Load("[ { id: 32768, name: a } ]"), "t", err));
    CHECK_FALSE(r.load(YAML::Load("[ { id: 1, name: a }, { id: 1, name: b } ]"), "t", err));
    CHECK(err.find("both a and b") != std::string::npos);
    CHECK_FALSE(r.load(YAML::Load("[ { id: 1, name: a }, { id: 2, name: a } ]"), "t", err));
    CHECK_FALSE(r.load(YAML::Load("[ { id: 1, name: \"a:b\" } ]"), "t", err));
    CHECK_FALSE(r.load(YAML::Load("{ a: 1 }"), "t", err));
}

TEST_CASE("topics expand into numbered partitions, entries merged over the topic") {
    const TopicRegistry reg = registry("[ { id: 11, name: crypto.binance }, { id: 20, name: eq.us } ]");
    const YAML::Node topics = YAML::Load(R"(
- name: crypto.binance
  transport: { type: aeron, channel: "aeron:ipc" }
  decoder: { type: sbe_routing, schema: schemas/subplane_canonical.xml, fields: { symbol: InstrumentId } }
  heartbeat_timeout_ms: 5000
  partitions:
    - {}
    - { heartbeat_timeout_ms: 100 }
    - transport: { stream_id: 4242 }
- name: eq.us
  transport: { type: udp_multicast, port: 30001, group: 239.1.1.1 }
  decoder: { type: sbe_routing, schema: schemas/subplane_canonical.xml }
)");
    std::vector<PartitionConfig> out;
    std::string err;
    REQUIRE_MESSAGE(expand_topics(topics, reg, repo_dir(), out, err), err);
    REQUIRE(out.size() == 4);
    CHECK(out[0].name == "crypto.binance/1");
    CHECK(out[0].id == 0);
    CHECK(out[0].topic_id == 11);
    CHECK(out[0].partition_num == 1);
    CHECK(out[0].stream_id == 720897);
    CHECK(out[0].transport_cfg["stream_id"].as<int32_t>() == 720897);   // derived Aeron stream id
    CHECK(out[0].transport_cfg["channel"].as<std::string>() == "aeron:ipc");
    CHECK(out[0].heartbeat_timeout_ms == 5000);
    CHECK(out[1].name == "crypto.binance/2");
    CHECK(out[1].stream_id == 720898);
    CHECK(out[1].heartbeat_timeout_ms == 100);                          // scalar override
    CHECK(out[2].transport_cfg["stream_id"].as<int32_t>() == 4242);     // explicit Aeron id kept
    CHECK(out[2].transport_cfg["channel"].as<std::string>() == "aeron:ipc"); // map merged, not replaced
    CHECK(out[2].stream_id == 720899);                                  // the stream's name still derives
    CHECK(out[3].name == "eq.us/1");                                    // no partitions: one
    CHECK(out[3].id == 3);
    CHECK(out[3].topic_id == 20);
    CHECK_FALSE(out[3].transport_cfg["stream_id"].IsDefined());         // only Aeron gets one

    const auto fails = [&](const char* yaml, const char* what) {
        std::vector<PartitionConfig> o;
        std::string e;
        CHECK_FALSE(expand_topics(YAML::Load(yaml), reg, repo_dir(), o, e));
        CHECK_MESSAGE(e.find(what) != std::string::npos, e);
    };
    fails("[ { name: nope, transport: { type: udp }, decoder: { type: sbe_routing } } ]", "not in the topic registry");
    fails("[ { name: eq.us, id: 20, transport: { type: udp }, decoder: { type: sbe_routing } } ]", "comes from the registry");
    fails("[ { name: eq.us, transport: { type: udp }, decoder: { type: sbe_routing }, partitions: [] } ]", "non-empty");
    fails("[ { name: eq.us, transport: { type: udp }, decoder: { type: sbe_routing }, partitions: [ { name: x } ] } ]", "no name of its own");
    fails("[ { name: \"eq:us\", transport: { type: udp }, decoder: { type: sbe_routing } } ]", "may only contain");
    fails("[ { name: eq.us, transport: { type: udp }, decoder: { type: sbe_routing } }, { name: eq.us, transport: { type: udp }, decoder: { type: sbe_routing } } ]", "listed twice");
}

namespace {

// Two partitions of one canonical topic, a session on it, messages pushed per partition.
struct TwoPartitions {
    SymbolTable symbols{1024};
    Lvc lvc;
    Partition part[2];
    FakeTransport* fake[2] = {};
    Pipeline pipe{"g0", 4242, symbols, lvc};
    std::string ring_path = "/dev/shm/subplane.test.topics." + std::to_string(getpid()) + ".ring";
    std::unique_ptr<Session> s;
    ShmReader rd;

    TwoPartitions() {
        std::string err;
        REQUIRE(lvc.init(1024, {static_cast<uint8_t>(MsgType::TRADE)}, 256, err));
        const TopicRegistry reg = registry("[ { id: 7, name: t.canon } ]");
        std::vector<PartitionConfig> cfgs;
        REQUIRE_MESSAGE(expand_topics(YAML::Load(R"(
- name: t.canon
  transport: { type: udp, group: "", port: 1 }
  framing: { length: schema, sequence: message.Sequence }
  decoder:
    type: sbe_routing
    schema: schemas/subplane_canonical.xml
    fields: { symbol: InstrumentId, sequence: Sequence, venue: VenueId, stream_id: StreamId }
    messages: [ { template: 1, msg_type: TRADE }, { template: 100, admin: HEARTBEAT } ]
  heartbeat_timeout_ms: 50
  partitions: [ {}, {} ]
)"), reg, repo_dir(), cfgs, err), err);
        REQUIRE(cfgs.size() == 2);
        for (size_t i = 0; i < 2; ++i) {
            auto ft = std::make_unique<FakeTransport>();
            fake[i] = ft.get();
            REQUIRE_MESSAGE(part[i].build(cfgs[i], err, std::move(ft)), err);
            REQUIRE(part[i].open(err));
            pipe.add_partition(&part[i]);
        }
        FilterSpec fs;
        fs.all_symbols = true;
        fs.msg_types = {static_cast<uint8_t>(MsgType::TRADE)};
        fs.topic_ids = {7};
        s = std::make_unique<Session>();
        s->id = 1; s->name = "t"; s->owner = "config"; s->partition_mask = 3u;
        s->filter_owned = build_filter(fs, 1024, 1, err);
        REQUIRE_MESSAGE(s->filter_owned, err);
        s->filter.store(s->filter_owned.get());
        RingConfig rc;
        rc.capacity_bytes = 1u << 20;
        rc.max_payload = 4096;
        rc.session_name = "t";
        REQUIRE_MESSAGE(s->ring.open(rc, ring_path, 4242, err), err);
        PipelineCommand c;
        c.type = PipelineCommand::Type::ADD_SESSION;
        c.session = s.get();
        REQUIRE(pipe.post(c));
        pipe.run_once();
        REQUIRE(rd.open(ring_path, &err));
    }
    ~TwoPartitions() { RingWriter::unlink_ring(ring_path); }

    std::vector<uint8_t> msg(uint16_t tid, uint64_t seq, uint32_t instrument, int32_t stream = 0) {
        const SbeSchema& sc = *part[0].schema();
        const MessageDef* m = sc.find_message(tid);
        REQUIRE(m);
        std::vector<uint8_t> b(8 + m->block_length, 0);
        const uint16_t hdr[4] = {m->block_length, tid, sc.schema_id(), sc.version()};
        std::memcpy(b.data(), hdr, 8);
        std::memcpy(b.data() + 8, &instrument, 4);
        std::memcpy(b.data() + 8 + 8, &seq, 8);
        if (tid == 100) std::memcpy(b.data() + 8 + 32, &stream, 4);
        return b;
    }
    std::vector<std::pair<TickHeader, std::vector<uint8_t>>> drain() {
        std::vector<std::pair<TickHeader, std::vector<uint8_t>>> out;
        RecordView rv;
        while (rd.poll(rv) == ReaderStatus::Ok) {
            if (rv.hdr->msg_type == static_cast<uint8_t>(ControlCode::AGENT_RESTARTED) ||
                rv.hdr->msg_type == static_cast<uint8_t>(ControlCode::SYMBOL_MAPPED)) continue;
            out.emplace_back(*rv.hdr, std::vector<uint8_t>(rv.payload, rv.payload + rv.payload_len));
        }
        return out;
    }
};

} // namespace

TEST_CASE("a topic's partitions: records carry topic_id, topic state counts degraded partitions") {
    TwoPartitions h;
    h.fake[0]->push(h.msg(1, 1, 5), now_ns());
    h.fake[1]->push(h.msg(1, 1, 6), now_ns());   // each stream has its own sequence: no gap
    h.pipe.run_once();
    auto recs = h.drain();
    REQUIRE(recs.size() == 2);
    for (const auto& [hdr, _] : recs) CHECK(hdr.topic_id == 7);
    CHECK(h.part[0].counters.snapshot().gaps == 0);
    CHECK(h.part[1].counters.snapshot().gaps == 0);

    // Partition 1 goes silent: the topic is degraded, 1 of 2.
    h.fake[0]->push(h.msg(1, 2, 5), now_ns() - 100'000'000ull);
    h.fake[1]->push(h.msg(1, 2, 6), now_ns());
    h.pipe.run_once();
    recs = h.drain();
    TopicStateEvent ev{};
    bool degraded = false;
    for (const auto& [hdr, body] : recs) {
        if (hdr.msg_type != static_cast<uint8_t>(ControlCode::TOPIC_DEGRADED)) continue;
        REQUIRE(body.size() == sizeof ev);
        std::memcpy(&ev, body.data(), sizeof ev);
        CHECK(hdr.topic_id == 7);
        degraded = true;
    }
    REQUIRE(degraded);
    CHECK(ev.topic_id == 7);
    CHECK(ev.degraded == 1);
    CHECK(ev.partitions == 2);
    CHECK(ev.reason == static_cast<uint32_t>(TopicReason::HEARTBEAT_TIMEOUT));

    // It comes back: RECOVERED, nothing degraded.
    h.fake[0]->push(h.msg(1, 3, 5), now_ns());
    h.pipe.run_once();
    recs = h.drain();
    bool recovered = false;
    for (const auto& [hdr, body] : recs) {
        if (hdr.msg_type != static_cast<uint8_t>(ControlCode::TOPIC_RECOVERED)) continue;
        std::memcpy(&ev, body.data(), sizeof ev);
        recovered = true;
    }
    REQUIRE(recovered);
    CHECK(ev.degraded == 0);
    CHECK(ev.partitions == 2);
}

TEST_CASE("a heartbeat stamped with another stream_id is counted, not dropped") {
    TwoPartitions h;
    h.fake[0]->push(h.msg(100, 1, 0, stream_id_of(7, 1)), now_ns());   // right stream
    h.fake[1]->push(h.msg(100, 1, 0, 0), now_ns());                    // unstamped
    h.pipe.run_once();
    CHECK(h.part[0].counters.snapshot().stream_mismatch == 0);
    CHECK(h.part[1].counters.snapshot().stream_mismatch == 0);
    h.fake[1]->push(h.msg(100, 2, 0, stream_id_of(7, 1)), now_ns());   // partition 2 wired to partition 1's publisher
    h.pipe.run_once();
    const auto c = h.part[1].counters.snapshot();
    CHECK(c.stream_mismatch == 1);
    CHECK(c.last_stream_stamp == static_cast<uint32_t>(stream_id_of(7, 1)));
    CHECK(c.admin == 2);   // still delivered as bus admin
}
