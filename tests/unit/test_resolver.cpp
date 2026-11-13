// The topic resolver (SPP Resolve): the recap manager's client, the pipeline's
// use of the homes and the topic check, and session ticker names.
#include <doctest.h>

#include "fake_recap.h"
#include "fake_transport.h"
#include "subplane/client/shm_reader.h"
#include "subplane/core/clock.h"
#include "subplane/pipeline/partition.h"
#include "subplane/pipeline/pipeline.h"
#include "subplane/pipeline/session_manager.h"
#include "subplane/pipeline/topic_registry.h"
#include "subplane/proto/subplane_proto_codec.h"
#include "subplane/recap/recap_manager.h"
#include "subplane/symbol_table.h"

#include <cstring>
#include <string>
#include <unistd.h>
#include <vector>

using namespace subplane;
using subplane::test::FakeRecap;
using subplane::test::FakeTransport;

namespace {

constexpr uint64_t kMs = 1'000'000ull;
constexpr uint16_t kTopicId = 7;
constexpr uint16_t kLast = 1;   // ResolveReply.Flags bit 0

std::string data_dir() {
    std::string f = __FILE__;
    return f.substr(0, f.find_last_of('/')) + "/../data";
}

std::string partition_yaml(bool recap, bool resolver) {
    std::string y = R"(
name: p
transport: { type: udp_multicast, group: "", port: 1 }
framing: { sequence: message.SeqNum }
decoder:
  type: sbe_routing
  schema: sample_md.xml
  fields: { symbol: SecurityID, sequence: SeqNum }
  symbol_id_mode: table
  messages:
    - { template: 1, msg_type: TRADE }
)";
    if (recap) y += "recap: { type: aeron, timeout_ms: 100, coalesce_ms: 0, reply_slabs: 2, reply_slab_bytes: 4096 }\n";
    if (resolver) y += "resolver: { type: aeron, timeout_ms: 100, refresh_ms: 1000 }\n";
    return y;
}

template <class Enc, class F> std::vector<uint8_t> spp(F fill) {
    std::vector<uint8_t> b(4 + 8192);
    Enc e(b.data() + 4, b.size() - 4);
    fill(e);
    const uint32_t n = static_cast<uint32_t>(e.finish());
    REQUIRE(n != 0);
    std::memcpy(b.data(), &n, 4);
    b.resize(4 + n);
    return b;
}

struct Item { uint64_t id; uint16_t partition; const char* symbol; };
std::vector<uint8_t> reply(uint64_t rid, uint16_t topic, uint16_t parts, std::initializer_list<Item> items, bool last) {
    return spp<sp_proto::ResolveReplyEncoder>([&](auto& e) {
        e.CorrelationId(rid).TopicId(topic).Partitions(parts).Flags(last ? kLast : 0);
        e.begin_Instruments(static_cast<uint16_t>(items.size()));
        uint16_t i = 0;
        for (const Item& it : items) e.Instruments(i++).Instrument(it.id).Venue(0).Partition(it.partition).Symbol(it.symbol);
    });
}

// The Resolve requests the client sent.
std::vector<uint64_t> resolves(const FakeRecap& f) {
    std::vector<uint64_t> out;
    for (const auto& fr : f.sent) {
        uint32_t n = 0;
        std::memcpy(&n, fr.data(), 4);
        sp_proto::Header h;
        REQUIRE(sp_proto::read_header(fr.data() + 4, n, h));
        if (h.template_id != sp_proto::ResolveDecoder::kTemplateId) continue;
        sp_proto::ResolveDecoder d;
        REQUIRE(d.wrap(fr.data() + 4, n));
        CHECK(d.Instrument() == 0);   // the whole topic
        CHECK(d.Name().empty());
        out.push_back(d.CorrelationId());
    }
    return out;
}

struct Rig {
    SymbolTable symbols{1024};
    Partition part;
    FakeRecap* resolver = nullptr;
    RecapChannel ch;
    TopicDirectory dir{"t", kTopicId};
    RecapManager mgr{1, symbols};
    uint64_t now = 1000 * kMs;

    explicit Rig(uint16_t configured = 2) {
        std::string err;
        PartitionConfig cfg;
        REQUIRE_MESSAGE(PartitionConfig::parse(YAML::Load(partition_yaml(false, true)), 0, data_dir(), cfg, err), err);
        cfg.set_topic("t", kTopicId, 1);
        auto fr = std::make_unique<FakeRecap>();
        resolver = fr.get();
        REQUIRE_MESSAGE(part.build(cfg, err, std::make_unique<FakeTransport>(), nullptr, std::move(fr)), err);
        REQUIRE(part.has_resolver());
        REQUIRE_MESSAGE(mgr.add_resolver(part, ch, dir, configured, err), err);
        REQUIRE_MESSAGE(mgr.open(err), err);
    }
    void step(uint64_t advance_ms = 0) { now += advance_ms * kMs; mgr.run_once(now); }
    std::vector<ResolveNote> notes() {
        std::vector<ResolveNote> out;
        ResolveNote n;
        while (ch.resolved.pop(n)) out.push_back(n);
        return out;
    }
    const ResolverCounters& c() const { return part.resolver_counters; }
};

} // namespace

TEST_CASE("resolver: config: one per topic, held by partition 1, no section stream") {
    TopicRegistry reg;
    std::string err;
    REQUIRE(reg.load(YAML::Load("[ { id: 7, name: t } ]"), "test", err));
    const std::string base = partition_yaml(false, true);
    YAML::Node topic = YAML::Load(base);
    topic.remove("name");
    topic["name"] = "t";
    topic["partitions"] = YAML::Load("[ {}, {} ]");
    YAML::Node topics(YAML::NodeType::Sequence);
    topics.push_back(topic);
    std::vector<PartitionConfig> cfgs;
    REQUIRE_MESSAGE(expand_topics(topics, reg, data_dir(), cfgs, err), err);
    REQUIRE(cfgs.size() == 2);
    CHECK(cfgs[0].resolver_node.IsMap());
    CHECK_FALSE(cfgs[1].resolver_node.IsMap());   // a default node is Null, not undefined

    topic["partitions"] = YAML::Load("[ {}, { resolver: { type: aeron } } ]");
    topics[0] = topic;
    CHECK_FALSE(expand_topics(topics, reg, data_dir(), cfgs, err));
    CHECK(err.find("topic key") != std::string::npos);

    ResolverConfig rc;
    CHECK_FALSE(ResolverConfig::parse(YAML::Load("{ type: aeron, sections: { channel: x, stream_id: 1 } }"), rc, err));
    CHECK(err.find("section") != std::string::npos);
    CHECK_FALSE(ResolverConfig::parse(YAML::Load("{ type: aeron, timeout_ms: 5000, refresh_ms: 1000 }"), rc, err));
}

TEST_CASE("resolver: Hello, then the whole topic; a paged answer is published once complete") {
    Rig r;
    r.step();
    REQUIRE(r.resolver->sent.size() == 2);   // Hello, Resolve
    const auto rq = resolves(*r.resolver);
    REQUIRE(rq.size() == 1);
    CHECK(r.c().resolver_requests == 1);
    CHECK(r.c().connected == 1);

    r.resolver->push(reply(rq[0], kTopicId, 2, {{101, 1, "BTCUSDT"}, {102, 2, "ETHUSDT"}}, false));
    r.step();
    CHECK(r.dir.version() == 0);   // not complete yet
    r.resolver->push(reply(rq[0], kTopicId, 2, {{103, 2, "SOLUSDT"}}, true));
    r.step();
    CHECK(r.dir.version() == 1);
    CHECK(r.dir.size() == 3);
    ResolvedInstrument eth;
    REQUIRE(r.dir.find("ETHUSDT", eth));
    CHECK(eth.instrument == 102);
    CHECK(eth.partition_num == 2);
    CHECK(r.c().resolver_lists == 1);
    CHECK(r.c().instruments == 3);
    CHECK(r.c().resolver_mismatch == 0);

    const auto notes = r.notes();
    REQUIRE(notes.size() == 4);
    CHECK(notes[0].kind == ResolveNote::Kind::TOPIC);
    CHECK_FALSE(notes[0].mismatch);
    CHECK(notes[1].kind == ResolveNote::Kind::HOME);
    CHECK(notes[1].symbol_id == r.symbols.lookup_u64(101));   // interned by the client
    CHECK(notes[1].home_num == 1);
    CHECK(notes[2].home_num == 2);

    // The refresh asks again; an unchanged list notes nothing new.
    r.step(999);
    CHECK(resolves(*r.resolver).size() == 1);
    r.step(1);
    const auto rq2 = resolves(*r.resolver);
    REQUIRE(rq2.size() == 2);
    r.resolver->push(reply(rq2[1], kTopicId, 2, {{101, 1, "BTCUSDT"}, {102, 2, "ETHUSDT"}, {103, 1, "SOLUSDT"}}, true));
    r.step();
    CHECK(r.dir.version() == 2);
    const auto moved = r.notes();
    REQUIRE(moved.size() == 1);   // SOLUSDT's home changed
    CHECK(moved[0].symbol_id == r.symbols.lookup_u64(103));
    CHECK(moved[0].home_num == 1);
}

TEST_CASE("resolver: another topic id or partition count is a mismatch, noted once") {
    Rig r(3);   // the config says three partitions
    r.step();
    const auto rq = resolves(*r.resolver);
    REQUIRE(rq.size() == 1);
    r.resolver->push(reply(rq[0], kTopicId, 2, {}, true));
    r.step();
    CHECK(r.c().resolver_mismatch == 1);
    auto notes = r.notes();
    REQUIRE(notes.size() == 1);
    CHECK(notes[0].mismatch);
    CHECK(notes[0].partitions == 2);
    r.step(1000);
    r.resolver->push(reply(resolves(*r.resolver).back(), kTopicId, 2, {}, true));
    r.step();
    CHECK(r.notes().empty());   // the same answer: no second note
    CHECK(r.c().resolver_mismatch == 1);
}

TEST_CASE("resolver: a lost page times out and the whole topic is asked again; strays and refusals") {
    Rig r;
    r.step();
    const uint64_t first = resolves(*r.resolver).at(0);
    r.resolver->push(reply(first, kTopicId, 2, {{101, 1, "BTCUSDT"}}, false));
    r.step();
    r.step(100);   // the last page never came
    CHECK(r.c().resolver_timeouts == 1);
    r.step(100);
    const auto rq = resolves(*r.resolver);
    REQUIRE(rq.size() == 2);
    r.resolver->push(reply(first, kTopicId, 2, {{101, 1, "BTCUSDT"}}, true));   // the old request: stray
    r.step();
    CHECK(r.c().resolver_stray == 1);
    CHECK(r.dir.version() == 0);

    // A service that does not resolve answers Unsupported: asked again after refresh_ms.
    r.resolver->push(spp<sp_proto::StatusEncoder>([&](auto& e) { e.CorrelationId(rq[1]).Code(sp_proto::StatusCode::Unsupported); }));
    r.step();
    CHECK(r.c().resolver_rejects == 1);
    r.step(999);
    CHECK(resolves(*r.resolver).size() == 2);
    r.step(1);
    CHECK(resolves(*r.resolver).size() == 3);
}

TEST_CASE("resolver: a reconnect asks at once") {
    Rig r;
    r.step();
    const uint64_t rid = resolves(*r.resolver).at(0);
    r.resolver->push(reply(rid, kTopicId, 2, {}, true));
    r.step();
    r.resolver->link_up = false;
    r.step();
    CHECK(r.c().connected == 0);
    r.resolver->link_up = true;
    r.step();
    CHECK(resolves(*r.resolver).size() == 2);
}

// ---- the pipeline: homes and the topic check ----

namespace {

struct PipeRig {
    SymbolTable symbols{1024};
    Lvc lvc;
    Partition part[2];
    Pipeline pipe;
    std::unique_ptr<Session> s;
    std::string path;
    ShmReader rd;

    PipeRig() : pipe("g0", 4242, symbols, lvc) {
        std::string err;
        REQUIRE(lvc.init(1024, {static_cast<uint8_t>(MsgType::TRADE)}, 256, err));
        for (uint16_t i = 0; i < 2; ++i) {
            PartitionConfig cfg;
            REQUIRE_MESSAGE(PartitionConfig::parse(YAML::Load(partition_yaml(true, i == 0)), i, data_dir(), cfg, err), err);
            cfg.set_topic("t", kTopicId, static_cast<uint16_t>(i + 1));
            REQUIRE_MESSAGE(part[i].build(cfg, err, std::make_unique<FakeTransport>(64), std::make_unique<FakeRecap>(),
                                          i == 0 ? std::make_unique<FakeRecap>() : nullptr), err);
            REQUIRE(part[i].open(err));
            pipe.add_partition(&part[i]);
        }
    }
    void session(std::initializer_list<uint64_t> secs) {
        s = std::make_unique<Session>();
        s->id = 1;
        s->name = "r";
        s->partition_mask = 3;
        s->recap = true;
        FilterSpec fs;
        for (uint64_t sec : secs) fs.symbol_ids.push_back(symbols.intern_u64(sec));
        fs.msg_types = {1};
        fs.topic_ids = {kTopicId};
        fs.agent_control = true;
        std::string err;
        s->filter_owned = build_filter(fs, 1024, 1, err);
        REQUIRE_MESSAGE(s->filter_owned, err);
        s->filter.store(s->filter_owned.get());
        path = "/dev/shm/subplane.resolvertest." + std::to_string(getpid()) + ".ring";
        RingConfig rc;
        rc.capacity_bytes = 1u << 20;
        rc.max_payload = 4096;
        rc.session_name = s->name;
        RingWriter::unlink_ring(path);
        REQUIRE_MESSAGE(s->ring.open(rc, path, 4242, err), err);
        REQUIRE(rd.open(path));
        PipelineCommand c;
        c.type = PipelineCommand::Type::ADD_SESSION;
        c.session = s.get();
        REQUIRE(pipe.post(c));
        pipe.run_once();
    }
    ~PipeRig() { if (s) { s->ring.close(); RingWriter::unlink_ring(path); } }
    void home(uint64_t sec, uint16_t num) {
        ResolveNote n;
        n.kind = ResolveNote::Kind::HOME;
        n.partition = part[0].id();
        n.symbol_id = symbols.intern_u64(sec);
        n.home_num = num;
        REQUIRE(pipe.recap_channel().resolved.push(n));
    }
    std::vector<RecapCommand> requests() {
        std::vector<RecapCommand> out;
        RecapCommand c;
        while (pipe.recap_channel().requests.pop(c)) if (c.op != RecapCommand::Op::FLOOR) out.push_back(c);
        return out;
    }
};

} // namespace

TEST_CASE("resolver: a listed home sends recap for an unseen instrument to that partition only") {
    PipeRig r;
    r.home(11, 2);
    r.pipe.run_once();
    r.session({11, 12});   // 12 is not listed: every partition is asked
    const auto rq = r.requests();
    int to11 = 0, to12 = 0;
    for (const auto& c : rq) {
        if (c.op != RecapCommand::Op::REQUEST) continue;
        if (c.symbol_id == r.symbols.lookup_u64(11)) { ++to11; CHECK(c.partition == r.part[1].id()); }
        if (c.symbol_id == r.symbols.lookup_u64(12)) ++to12;
    }
    CHECK(to11 == 1);
    CHECK(to12 == 2);
}

TEST_CASE("resolver: a mismatch is a TOPIC_MISMATCH event for the topic's sessions") {
    PipeRig r;
    r.session({11});
    RecordView v;
    while (r.rd.poll(v) == ReaderStatus::Ok) {}   // AGENT_RESTARTED, RECAP_PENDING
    ResolveNote n;
    n.kind = ResolveNote::Kind::TOPIC;
    n.mismatch = true;
    n.partition = r.part[0].id();
    n.topic_id = kTopicId;
    n.partitions = 3;
    REQUIRE(r.pipe.recap_channel().resolved.push(n));
    r.pipe.run_once();
    REQUIRE(r.rd.poll(v) == ReaderStatus::Ok);
    CHECK(v.hdr->msg_type == static_cast<uint8_t>(ControlCode::TOPIC_MISMATCH));
    TopicMismatchEvent ev;
    std::memcpy(&ev, v.payload, sizeof(ev));
    CHECK(ev.topic_id == kTopicId);
    CHECK(ev.partitions == 2);
    CHECK(ev.resolver_topic_id == kTopicId);
    CHECK(ev.resolver_partitions == 3);
}

// ---- session names ----

namespace {

struct NameRig {
    SymbolTable symbols{256};
    Lvc lvc;
    Partition part[2];
    Pipeline pipe;
    TopicDirectory a{"crypto.a", 21}, b{"crypto.b", 22};
    std::unique_ptr<SessionManager> sm;
    std::string agent_name;

    NameRig() : pipe("g0", 77, symbols, lvc) {
        std::string err;
        REQUIRE(lvc.init(256, {static_cast<uint8_t>(MsgType::TRADE)}, 256, err));
        const char* names[] = {"crypto.a", "crypto.b"};
        for (uint16_t i = 0; i < 2; ++i) {
            PartitionConfig cfg;
            REQUIRE_MESSAGE(PartitionConfig::parse(YAML::Load(partition_yaml(false, false)), i, data_dir(), cfg, err), err);
            cfg.set_topic(names[i], static_cast<uint16_t>(21 + i), 1);
            REQUIRE(part[i].build(cfg, err, std::make_unique<FakeTransport>()));
            REQUIRE(part[i].open(err));
            pipe.add_partition(&part[i]);
        }
        agent_name = "rstest" + std::to_string(getpid());
        SessionManagerConfig c;
        c.agent_name = agent_name;
        c.epoch = 77;
        c.max_symbols = 256;
        sm = std::make_unique<SessionManager>(c, symbols, lvc, std::vector<Partition*>{&part[0], &part[1]},
                                              std::vector<Pipeline*>{&pipe}, std::vector<TopicDirectory*>{&a, &b});
    }
    ~NameRig() { sm.reset(); }
    SessionSpec spec(const char* name, std::vector<std::string> syms) {
        SessionSpec s;
        s.name = name;
        s.owner = "config";
        s.ring_capacity_bytes = 1u << 20;
        s.max_payload = 1024;
        s.symbols = std::move(syms);
        s.msg_types = {"TRADE"};
        s.agent_control = true;
        return s;
    }
    std::vector<SymbolUnresolvedEvent> unresolved(ShmReader& rd) {
        std::vector<SymbolUnresolvedEvent> out;
        RecordView v;
        while (rd.poll(v) == ReaderStatus::Ok) {
            if (v.hdr->msg_type != static_cast<uint8_t>(ControlCode::SYMBOL_UNRESOLVED)) continue;
            SymbolUnresolvedEvent e;
            std::memcpy(&e, v.payload, sizeof(e));
            out.push_back(e);
        }
        return out;
    }
    std::string ring(const char* name) { return "/dev/shm/subplane." + agent_name + "." + name + ".ring"; }
};

std::string name_of(const SymbolUnresolvedEvent& e) { return std::string(e.name, e.name_len); }

} // namespace

TEST_CASE("names: unique, ambiguous, qualified, unknown") {
    NameRig r;
    r.a.publish({{101, 0, 1, "BTCUSDT"}, {102, 0, 1, "ETHUSDT"}});
    r.b.publish({{201, 0, 1, "BTCUSDT"}, {203, 0, 1, "SOLUSDT"}});
    std::string err;
    REQUIRE_MESSAGE(r.sm->create(r.spec("n", {"ETHUSDT", "SOLUSDT", "BTCUSDT", "crypto.b:BTCUSDT", "XRPUSDT", "7"}), err), err);
    FilterSpec fs;
    uint64_t mask = 0;
    Pipeline* g = nullptr;
    std::vector<UnresolvedName> un;
    REQUIRE(r.sm->resolve(r.spec("n", {"ETHUSDT", "SOLUSDT", "BTCUSDT", "crypto.b:BTCUSDT", "XRPUSDT", "7"}), fs, mask, g, err, &un));
    // ETHUSDT on a, SOLUSDT on b, the qualified BTCUSDT on b, and 7 as an id.
    REQUIRE(fs.symbol_ids.size() == 4);
    CHECK(fs.symbol_ids[0] == r.symbols.lookup_u64(102));
    CHECK(fs.symbol_ids[1] == r.symbols.lookup_u64(203));
    CHECK(fs.symbol_ids[2] == r.symbols.lookup_u64(201));
    REQUIRE(un.size() == 2);
    CHECK(un[0].name == "BTCUSDT");
    CHECK(un[0].reason == UnresolvedReason::AMBIGUOUS);
    CHECK(un[1].name == "XRPUSDT");
    CHECK(un[1].reason == UnresolvedReason::UNKNOWN);

    ShmReader rd;
    REQUIRE(rd.open(r.ring("n")));
    const auto ev = r.unresolved(rd);
    REQUIRE(ev.size() == 2);
    CHECK(name_of(ev[0]) == "BTCUSDT");
    CHECK(ev[0].reason == static_cast<uint8_t>(UnresolvedReason::AMBIGUOUS));
    CHECK(name_of(ev[1]) == "XRPUSDT");
    CHECK(ev[1].reason == static_cast<uint8_t>(UnresolvedReason::UNKNOWN));

    // A refresh with the same lists: nothing new, no filter swap.
    r.a.publish({{101, 0, 1, "BTCUSDT"}, {102, 0, 1, "ETHUSDT"}});
    r.sm->service_names();
    SessionAttachInfo info;
    REQUIRE(r.sm->attach_info("n", info));
    CHECK(info.filter_generation == 1);
    CHECK(r.unresolved(rd).empty());

    // XRPUSDT gets listed: it joins the filter.
    r.b.publish({{201, 0, 1, "BTCUSDT"}, {203, 0, 1, "SOLUSDT"}, {204, 0, 1, "XRPUSDT"}});
    r.sm->service_names();
    REQUIRE(r.sm->attach_info("n", info));
    CHECK(info.filter_generation == 2);
    CHECK(r.unresolved(rd).empty());
}

TEST_CASE("names: before a topic's list arrives a name waits silently, then resolves") {
    NameRig r;
    r.a.publish({{101, 0, 1, "BTCUSDT"}});   // b has no list yet
    std::string err;
    REQUIRE_MESSAGE(r.sm->create(r.spec("w", {"SOLUSDT"}), err), err);
    ShmReader rd;
    REQUIRE(rd.open(r.ring("w")));
    CHECK(r.unresolved(rd).empty());   // not reported: b may list it
    r.b.publish({{203, 0, 1, "SOLUSDT"}});
    r.sm->service_names();
    SessionAttachInfo info;
    REQUIRE(r.sm->attach_info("w", info));
    CHECK(info.filter_generation == 2);
    CHECK(r.unresolved(rd).empty());
}

TEST_CASE("names: topics without a resolver keep string symbols") {
    NameRig r;
    // A session on topics without a directory: a name is a string symbol, as before.
    TopicDirectory none{"other", 99};
    FilterSpec fs;
    uint64_t mask = 0;
    Pipeline* g = nullptr;
    std::string err;
    SessionManagerConfig c;
    c.agent_name = r.agent_name + "x";
    c.max_symbols = 256;
    SessionManager plain(c, r.symbols, r.lvc, std::vector<Partition*>{&r.part[0], &r.part[1]}, std::vector<Pipeline*>{&r.pipe});
    REQUIRE(plain.resolve(r.spec("s", {"BTCUSDT"}), fs, mask, g, err));
    REQUIRE(fs.symbol_ids.size() == 1);
    CHECK(fs.symbol_ids[0] == r.symbols.lookup("BTCUSDT"));
}
