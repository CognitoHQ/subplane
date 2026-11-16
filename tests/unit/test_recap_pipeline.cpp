#include <doctest.h>

#include "fake_recap.h"
#include "fake_transport.h"
#include "subplane/client/shm_reader.h"
#include "subplane/core/clock.h"
#include "subplane/pipeline/pipeline.h"
#include "subplane/pipeline/recap_state.h"
#include "subplane/pipeline/session_manager.h"
#include "subplane/proto/subplane_proto_codec.h"
#include "subplane/shm/control_events.h"

#include <cstring>
#include <deque>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

using namespace subplane;
using subplane::test::FakeRecap;
using subplane::test::FakeTransport;

// The test plays the recap manager: it pops the pipeline's requests and pushes
// replies whose slabs it owns.

namespace {

std::string data_dir() {
    std::string f = __FILE__;
    return f.substr(0, f.find_last_of('/')) + "/../data";
}

std::string partition_yaml(const std::string& recap_extra = "") {
    return R"(
name: p0
transport: { type: udp_multicast, group: "", port: 1 }
framing: { sequence: message.SeqNum }
decoder:
  type: sbe_routing
  schema: sample_md.xml
  fields: { symbol: SecurityID, sequence: SeqNum }
  symbol_id_mode: table
  messages:
    - { template: 1,   msg_type: TRADE }
    - { template: 2,   msg_type: QUOTE }
    - { template: 101, admin: RECAP_BEGIN, fields: { request_id: SeqNum, as_of: NewSeq } }
    - { template: 100, admin: RECAP_END,   fields: { request_id: SeqNum } }
recap:
  type: aeron
)" + recap_extra;
}

void put(std::vector<uint8_t>& b, uint64_t v, unsigned size) {
    for (unsigned i = 0; i < size; ++i) b.push_back(static_cast<uint8_t>(v >> (8 * i)));
}
void header(std::vector<uint8_t>& b, uint16_t blk, uint16_t tid) { put(b, blk, 2); put(b, tid, 2); put(b, 7, 2); put(b, 2, 2); }

// Trade: SecurityID, SeqNum, TransactTime (used as a tag to tell messages apart), MarketSegmentID.
std::vector<uint8_t> trade(uint64_t sec, uint64_t seq, uint64_t tag = 0) {
    std::vector<uint8_t> b; header(b, 39, 1);
    put(b, sec, 8); put(b, seq, 8); put(b, tag, 8); put(b, 0, 2);
    b.insert(b.end(), 39 - 26, 0);
    return b;
}
std::vector<uint8_t> quote(uint64_t sec, uint64_t seq) {
    std::vector<uint8_t> b; header(b, 56, 2);
    put(b, sec, 8); put(b, seq, 8);
    b.insert(b.end(), 56 - 16, 0);
    return b;
}

struct Rec { TickHeader hdr; std::vector<uint8_t> payload; };

bool is(const Rec& r, ControlCode c) { return r.hdr.kind == static_cast<uint8_t>(Kind::AGENT_CONTROL) && r.hdr.msg_type == static_cast<uint8_t>(c); }
RecapEvent recap_ev(const Rec& r) { RecapEvent ev{}; REQUIRE(r.payload.size() == sizeof(ev)); std::memcpy(&ev, r.payload.data(), sizeof(ev)); return ev; }
uint64_t tag(const Rec& r) { uint64_t t; std::memcpy(&t, r.payload.data() + 8 + 16, 8); return t; }

struct App {
    std::unique_ptr<Session> s;
    std::string path;
    ShmReader rd;
    ~App() { RingWriter::unlink_ring(path); }

    // Everything new on the ring, without SYMBOL_MAPPED and AGENT_RESTARTED.
    std::vector<Rec> drain() {
        std::vector<Rec> out;
        RecordView v;
        while (rd.poll(v) == ReaderStatus::Ok) {
            if (v.hdr->kind == static_cast<uint8_t>(Kind::AGENT_CONTROL) &&
                (v.hdr->msg_type == static_cast<uint8_t>(ControlCode::SYMBOL_MAPPED) ||
                 v.hdr->msg_type == static_cast<uint8_t>(ControlCode::AGENT_RESTARTED))) continue;
            Rec r;
            r.hdr = *v.hdr;
            r.payload.assign(v.payload, v.payload + v.payload_len);
            out.push_back(std::move(r));
        }
        return out;
    }
};

struct Rig {
    SymbolTable symbols{1024};
    Lvc lvc;
    Partition part;
    FakeTransport* fake = nullptr;
    Pipeline pipe;
    std::deque<std::vector<uint8_t>> slabs; // replies' bytes, owned here
    uint64_t seq = 0;
    uint32_t next_id = 1;

    explicit Rig(const std::string& yaml = partition_yaml()) : pipe("g0", 4242, symbols, lvc) {
        std::string err;
        REQUIRE(lvc.init(1024, {static_cast<uint8_t>(MsgType::TRADE)}, 256, err));
        PartitionConfig cfg;
        REQUIRE_MESSAGE(PartitionConfig::parse(YAML::Load(yaml), 0, data_dir(), cfg, err), err);
        cfg.set_topic("t", 1, 1);
        auto ft = std::make_unique<FakeTransport>(64);
        fake = ft.get();
        REQUIRE_MESSAGE(part.build(cfg, err, std::move(ft), std::make_unique<FakeRecap>()), err);
        REQUIRE(part.open(err));
        pipe.add_partition(&part);
    }

    uint32_t sym(uint64_t sec) { return symbols.intern_u64(sec); }

    std::unique_ptr<App> app(std::initializer_list<uint64_t> secs, bool recap, std::vector<uint8_t> types = {1, 2}) {
        auto a = std::make_unique<App>();
        a->s = std::make_unique<Session>();
        Session& s = *a->s;
        s.id = next_id++;
        s.name = "s" + std::to_string(s.id);
        s.partition_mask = 1;
        s.recap = recap;
        FilterSpec fs;
        for (uint64_t sec : secs) fs.symbol_ids.push_back(sym(sec));
        fs.msg_types = types;
        fs.topic_ids = {1};
        std::string err;
        s.filter_owned = build_filter(fs, 1024, 1, err);
        REQUIRE_MESSAGE(s.filter_owned, err);
        s.filter.store(s.filter_owned.get());
        a->path = "/dev/shm/subplane.recaptest." + std::to_string(getpid()) + "." + s.name + ".ring";
        RingConfig rc;
        rc.capacity_bytes = 1u << 20;
        rc.max_payload = 4096;
        rc.session_name = s.name;
        RingWriter::unlink_ring(a->path);
        REQUIRE_MESSAGE(s.ring.open(rc, a->path, 4242, err), err);
        REQUIRE(a->rd.open(a->path));
        command(PipelineCommand::Type::ADD_SESSION, &s);
        return a;
    }
    void command(PipelineCommand::Type t, Session* s, const FilterTable* f = nullptr, uint32_t gen = 0) {
        PipelineCommand c;
        c.type = t;
        c.session = s;
        c.filter = f;
        c.generation = gen;
        REQUIRE(pipe.post(c));
        pipe.run_once();
    }
    void set_symbols(App& a, std::initializer_list<uint64_t> secs) {
        FilterSpec fs;
        for (uint64_t sec : secs) fs.symbol_ids.push_back(sym(sec));
        fs.msg_types = {1, 2};
        fs.topic_ids = {1};
        std::string err;
        auto fresh = build_filter(fs, 1024, 2, err);
        REQUIRE(fresh);
        command(PipelineCommand::Type::SWAP_FILTER, a.s.get(), fresh.get(), 2);
        a.s->filter_retiring = std::move(a.s->filter_owned);
        a.s->filter_owned = std::move(fresh);
    }

    // One live trade per call, next sequence.
    void live(uint64_t sec, uint64_t t = 0) {
        fake->push(trade(sec, ++seq, t), now_ns());
        pipe.run_once();
    }

    // REQUEST and CANCEL; FLOOR updates are set aside in `floors`.
    std::vector<RecapCommand> floors;
    std::vector<RecapCommand> requests() {
        std::vector<RecapCommand> out;
        RecapCommand c;
        while (pipe.recap_channel().requests.pop(c)) (c.op == RecapCommand::Op::FLOOR ? floors : out).push_back(c);
        return out;
    }
    std::vector<RecapRelease> releases() {
        std::vector<RecapRelease> out;
        RecapRelease r;
        while (pipe.recap_channel().releases.pop(r)) out.push_back(r);
        return out;
    }

    // `generation` < 0: the partition's current one, as a reply to its latest request.
    void reply(uint64_t sec, uint64_t as_of, const std::vector<std::vector<uint8_t>>& records, uint16_t slab = 3,
               uint32_t attempt = 1, int64_t generation = -1) {
        std::vector<uint8_t> buf;
        for (const auto& m : records) { put(buf, m.size(), 4); buf.insert(buf.end(), m.begin(), m.end()); }
        slabs.push_back(std::move(buf));
        RecapReply r;
        r.partition = 0;
        r.slab = slab;
        r.symbol_id = sym(sec);
        r.request_id = 900 + slab;
        r.attempt = attempt;
        r.generation = generation < 0 ? part.recap->generation() : static_cast<uint32_t>(generation);
        r.status = RecapStatus::OK;
        r.as_of = as_of;
        r.data = slabs.back().data();
        r.bytes = static_cast<uint32_t>(slabs.back().size());
        r.messages = static_cast<uint32_t>(records.size());
        REQUIRE(pipe.recap_channel().replies.push(r));
        pipe.run_once();
    }
    void failure(uint64_t sec, RecapStatus st, uint32_t attempt) {
        RecapReply r;
        r.partition = 0;
        r.symbol_id = sym(sec);
        r.request_id = 77;
        r.attempt = attempt;
        r.generation = part.recap->generation();
        r.status = st;
        REQUIRE(pipe.recap_channel().replies.push(r));
        pipe.run_once();
    }
};

} // namespace

TEST_CASE("recap state: hash index survives collisions and deletes; holds span chunks and free in one step") {
    RecapConfig cfg;
    cfg.max_pending = 64;
    cfg.hold_bytes = 64 * 16;
    cfg.hold_chunk_bytes = 64;
    RecapState st;
    st.init(cfg);
    for (uint32_t i = 1; i <= 64; ++i) REQUIRE(st.insert(i * 128, 0)); // same low bits
    CHECK(st.insert(99999, 0) == nullptr);
    for (uint32_t i = 1; i <= 64; i += 2) st.erase(*st.find(i * 128));
    for (uint32_t i = 1; i <= 64; ++i) CHECK((st.find(i * 128) != nullptr) == (i % 2 == 0));
    CHECK(st.pending() == 32);

    RecapPending& e = *st.find(256);
    std::vector<uint8_t> payload(100);
    for (size_t i = 0; i < payload.size(); ++i) payload[i] = static_cast<uint8_t>(i);
    TickHeader h{};
    for (uint64_t s = 1; s <= 6; ++s) {
        h.sequence = s;
        REQUIRE(st.hold(e, h, h.sequence, payload.data(), 100)); // 164 bytes each: 6 fit in 16 chunks of 64
    }
    h.sequence = 7;
    CHECK_FALSE(st.hold(e, h, h.sequence, payload.data(), 100));
    uint64_t want = 1;
    st.for_each_held(e, [&](const TickHeader& hh, const uint8_t* p, uint32_t len) {
        CHECK(hh.sequence == want++);
        REQUIRE(len == 100);
        CHECK(std::memcmp(p, payload.data(), 100) == 0);
    });
    CHECK(want == 7);
    st.free_holds(e);
    CHECK(st.free_chunks() == st.total_chunks());
    REQUIRE(st.hold(e, h, h.sequence, payload.data(), 100));
}

TEST_CASE("recap merge: snapshot, COMPLETE, then the held messages above S; live sessions are never delayed") {
    Rig r;
    auto live_app = r.app({1001}, false);
    r.live(1001, 1); // seq 1
    r.live(1001, 2); // seq 2
    auto rec = r.app({1001}, true);
    auto reqs = r.requests();
    REQUIRE(reqs.size() == 1);
    CHECK(reqs[0].op == RecapCommand::Op::REQUEST);
    CHECK(reqs[0].symbol_id == r.sym(1001));
    auto got = rec->drain();
    REQUIRE(got.size() == 1);
    CHECK(is(got[0], ControlCode::RECAP_PENDING));
    CHECK(recap_ev(got[0]).symbol_id == r.sym(1001));

    r.live(1001, 3); // seq 3, held
    r.live(1001, 4); // seq 4, held
    r.live(2002, 5); // another symbol: not held, not in the recap session's filter
    CHECK(rec->drain().empty());
    CHECK(r.part.recap_counters.recap_held.load() == 2);

    r.reply(1001, 3, {trade(1001, 0, 100), quote(1001, 0)});
    got = rec->drain();
    REQUIRE(got.size() == 5);
    CHECK(is(got[0], ControlCode::RECAP_BEGIN));
    CHECK(recap_ev(got[0]).as_of_seq == 3);
    CHECK(recap_ev(got[0]).record_count == 2);
    CHECK(recap_ev(got[0]).attempt == 1);
    for (size_t i : {size_t{1}, size_t{2}}) {
        CHECK(got[i].hdr.kind == static_cast<uint8_t>(Kind::DATA));
        CHECK(got[i].hdr.sequence == 3);
        CHECK((got[i].hdr.flags & FLAG_SNAPSHOT_ORIGIN) != 0);
    }
    CHECK(tag(got[1]) == 100);
    CHECK(got[2].hdr.msg_type == static_cast<uint8_t>(MsgType::QUOTE));
    CHECK(is(got[3], ControlCode::RECAP_COMPLETE));
    CHECK(recap_ev(got[3]).record_count == 2);
    CHECK(got[4].hdr.sequence == 4); // seq 3 is in the snapshot: not delivered twice
    CHECK((got[4].hdr.flags & FLAG_SNAPSHOT_ORIGIN) == 0);
    CHECK(tag(got[4]) == 4);
    auto rel = r.releases();
    REQUIRE(rel.size() == 1);
    CHECK(rel[0].slab == 3);
    CHECK(r.part.recap->pending() == 0);
    CHECK(r.part.recap_counters.recap_merged.load() == 1);

    r.live(1001, 6);
    got = rec->drain();
    REQUIRE(got.size() == 1);
    CHECK(got[0].hdr.sequence == 6);

    // The live session saw every message as it arrived, and nothing from the snapshot.
    auto all = live_app->drain();
    std::vector<uint64_t> seqs;
    for (auto& x : all) if (x.hdr.kind == static_cast<uint8_t>(Kind::DATA)) seqs.push_back(x.hdr.sequence);
    CHECK(seqs == std::vector<uint64_t>{1, 2, 3, 4, 6});
    CHECK(r.requests().empty()); // no CANCEL after a merge
}

TEST_CASE("recap merge: a reply before anything is held, and a reply ahead of live waits for live to reach S") {
    Rig r;
    r.live(1001); r.live(1001); // seq 2
    auto rec = r.app({1001, 1002}, true);
    CHECK(r.requests().size() == 2);
    rec->drain();

    r.reply(1001, 2, {trade(1001, 0, 50)}, 1);
    auto got = rec->drain();
    REQUIRE(got.size() == 3);
    CHECK(is(got[0], ControlCode::RECAP_BEGIN));
    CHECK(tag(got[1]) == 50);
    CHECK(is(got[2], ControlCode::RECAP_COMPLETE));

    // S = 4 while live is at 2: parked.
    r.reply(1002, 4, {trade(1002, 0, 60)}, 2);
    CHECK(rec->drain().empty());
    CHECK(r.part.recap->parked() == 1);
    r.live(1002, 3); // seq 3: held, already in the snapshot
    CHECK(rec->drain().empty());
    r.live(1002, 4); // seq 4: live reaches S, the merge runs in the same loop
    got = rec->drain();
    REQUIRE(got.size() == 3);
    CHECK(is(got[0], ControlCode::RECAP_BEGIN));
    CHECK(tag(got[1]) == 60);
    CHECK(is(got[2], ControlCode::RECAP_COMPLETE));
    r.live(1002, 5);
    got = rec->drain();
    REQUIRE(got.size() == 1);
    CHECK(got[0].hdr.sequence == 5);
    CHECK(r.releases().size() == 2);
}

TEST_CASE("recap merge: a snapshot older than the floor fails as too old and is asked for again") {
    Rig r;
    for (int i = 0; i < 5; ++i) r.live(1001); // seq 5
    auto rec = r.app({1001}, true);
    r.requests();
    rec->drain();
    r.live(1001, 6);
    r.reply(1001, 3, {trade(1001, 0)}, 4, 1);
    auto got = rec->drain();
    REQUIRE(got.size() == 1);
    CHECK(is(got[0], ControlCode::RECAP_FAILED));
    CHECK(recap_ev(got[0]).status == static_cast<uint8_t>(RecapEventStatus::TOO_OLD));
    CHECK(recap_ev(got[0]).as_of_seq == 3);
    auto reqs = r.requests();
    REQUIRE(reqs.size() == 1);
    CHECK(reqs[0].op == RecapCommand::Op::REQUEST);
    CHECK(r.releases().size() == 1);
    CHECK(r.part.recap_counters.recap_too_old.load() == 1);

    r.reply(1001, 5, {trade(1001, 0, 70)}, 5, 2); // S = floor: everything after it is held
    got = rec->drain();
    REQUIRE(got.size() == 4);
    CHECK(recap_ev(got[0]).attempt == 2);
    CHECK(tag(got[1]) == 70);
    CHECK(got[3].hdr.sequence == 6);
    CHECK(tag(got[3]) == 6);
}

TEST_CASE("recap merge: hold overflow raises the floor, told once; a sequence gap raises it too") {
    Rig r(partition_yaml("  hold_bytes: 512\n  hold_chunk_bytes: 64\n"));
    r.live(1001); // seq 1
    auto rec = r.app({1001}, true);
    r.requests();
    rec->drain();
    // A held trade is 64 + 47 bytes = 2 chunks; 8 chunks hold 4.
    for (int i = 0; i < 6; ++i) r.live(1001); // seq 2..7: 2..5 held, 6 overflows (floor 6), 7 held
    auto got = rec->drain();
    REQUIRE(got.size() == 1);
    CHECK(is(got[0], ControlCode::RECAP_FAILED));
    CHECK(recap_ev(got[0]).status == static_cast<uint8_t>(RecapEventStatus::HOLD_OVERFLOW));
    CHECK(r.part.recap_counters.recap_hold_overflows.load() == 1);
    CHECK(r.part.recap->find(r.sym(1001))->floor == 6);

    r.reply(1001, 5, {trade(1001, 0)}, 1);
    got = rec->drain();
    REQUIRE(got.size() == 1);
    CHECK(recap_ev(got[0]).status == static_cast<uint8_t>(RecapEventStatus::TOO_OLD));
    REQUIRE(r.requests().size() == 1);

    // Gap: 8 and 9 never arrive. The request is renewed.
    r.seq = 9;
    r.live(1001); // seq 10
    CHECK(r.part.recap->find(r.sym(1001))->floor == 9);
    got = rec->drain();
    REQUIRE(got.size() == 2);
    CHECK(is(got[0], ControlCode::GAP_DETECTED));
    CHECK(is(got[1], ControlCode::SYMBOL_STALE));
    REQUIRE(r.requests().size() == 1);
    r.reply(1001, 8, {trade(1001, 0)}, 2);
    got = rec->drain();
    REQUIRE(got.size() == 1);
    CHECK(recap_ev(got[0]).status == static_cast<uint8_t>(RecapEventStatus::TOO_OLD));
    r.requests();
    r.reply(1001, 9, {trade(1001, 0, 90)}, 3);
    got = rec->drain();
    REQUIRE(got.size() == 4);
    CHECK(tag(got[1]) == 90);
    CHECK(got[3].hdr.sequence == 10);
}

TEST_CASE("recap merge: manager failures reach the waiters with the attempt; they stay pending") {
    Rig r;
    auto rec = r.app({1001}, true);
    rec->drain();
    r.failure(1001, RecapStatus::TIMEOUT, 1);
    r.failure(1001, RecapStatus::REJECTED, 2);
    r.failure(1001, RecapStatus::OVERFLOW, 3);
    auto got = rec->drain();
    REQUIRE(got.size() == 3);
    CHECK(recap_ev(got[0]).status == static_cast<uint8_t>(RecapEventStatus::TIMEOUT));
    CHECK(recap_ev(got[0]).attempt == 1);
    CHECK(recap_ev(got[1]).status == static_cast<uint8_t>(RecapEventStatus::REJECTED));
    CHECK(recap_ev(got[2]).status == static_cast<uint8_t>(RecapEventStatus::REPLY_TOO_LARGE));
    CHECK(recap_ev(got[2]).attempt == 3);
    CHECK(r.part.recap->pending() == 1);
    CHECK(r.requests().size() == 1); // only the first REQUEST: the manager retries by itself
}

TEST_CASE("recap waiters: one request per symbol, filter add and remove, CANCEL when the last waiter leaves") {
    Rig r;
    auto a = r.app({1001}, true);
    auto b = r.app({1001}, true);
    auto c = r.app({1001, 1002}, true);
    auto reqs = r.requests();
    REQUIRE(reqs.size() == 2); // 1001 once, 1002 once
    for (auto* x : {a.get(), b.get()}) {
        auto got = x->drain();
        REQUIRE(got.size() == 1);
        CHECK(is(got[0], ControlCode::RECAP_PENDING));
    }
    CHECK(c->drain().size() == 2);

    // a leaves 1001 for 1003: one new request, no cancel (b and c still wait on 1001).
    r.set_symbols(*a, {1003});
    reqs = r.requests();
    REQUIRE(reqs.size() == 1);
    CHECK(reqs[0].op == RecapCommand::Op::REQUEST);
    CHECK(reqs[0].symbol_id == r.sym(1003));
    auto got = a->drain();
    REQUIRE(got.size() == 2); // FILTER_RELOADED, RECAP_PENDING
    CHECK(is(got[0], ControlCode::FILTER_RELOADED));
    CHECK(is(got[1], ControlCode::RECAP_PENDING));

    // The merge reaches b and c only.
    r.live(1001);
    r.reply(1001, 0, {trade(1001, 0, 11)}, 1);
    CHECK(a->drain().empty());
    CHECK(b->drain().size() == 4);
    CHECK(c->drain().size() == 4);

    // c is removed: 1002 has no waiter left.
    r.command(PipelineCommand::Type::REMOVE_SESSION, c->s.get());
    reqs = r.requests();
    REQUIRE(reqs.size() == 1);
    CHECK(reqs[0].op == RecapCommand::Op::CANCEL);
    CHECK(reqs[0].symbol_id == r.sym(1002));
    // A late reply for the cancelled symbol is dropped and its slab released.
    r.reply(1002, 1, {trade(1002, 0)}, 6);
    REQUIRE(r.releases().size() == 2);
    CHECK(r.part.recap->pending() == 1); // 1003, for a
}

TEST_CASE("recap waiters: requests the full queue cannot take are posted later, in order") {
    Rig r;
    auto& q = r.pipe.recap_channel().requests;
    int filled = 0;
    while (q.push({RecapCommand::Op::CANCEL, 0, 999})) ++filled;
    auto rec = r.app({1001, 1002}, true);
    CHECK(rec->drain().size() == 2); // the sessions hear PENDING anyway
    RecapCommand c;
    for (int i = 0; i < filled; ++i) REQUIRE(q.pop(c));
    r.pipe.run_once();
    auto reqs = r.requests();
    REQUIRE(reqs.size() == 2);
    CHECK(reqs[0].symbol_id == r.sym(1001));
    CHECK(reqs[1].symbol_id == r.sym(1002));
}

TEST_CASE("recap sessions: validated at creation; recap is fixed afterwards") {
    Rig r(partition_yaml("  max_pending: 2\n"));
    SessionManagerConfig cfg;
    cfg.agent_name = "recaptest" + std::to_string(getpid());
    cfg.max_symbols = 1024;
    SessionManager sm(cfg, r.symbols, r.lvc, {&r.part}, {&r.pipe});
    std::string err;
    SessionSpec s;
    s.name = "r1";
    s.ring_capacity_bytes = 1u << 16;
    s.max_payload = 1024;
    s.recap = true;
    s.all_symbols = true;
    CHECK_FALSE(sm.create(s, err));
    CHECK(err.find("symbol list") != std::string::npos);
    s.all_symbols = false;
    s.agent_control = false;
    s.symbols = {"1001"};
    CHECK_FALSE(sm.create(s, err));
    CHECK(err.find("agent control") != std::string::npos);
    s.agent_control = true;
    REQUIRE_MESSAGE(sm.create(s, err), err);
    SessionSpec t = s;
    t.name = "r2";
    t.symbols = {"1001", "1002"}; // 2 distinct with r1: fits
    REQUIRE_MESSAGE(sm.create(t, err), err);
    SessionSpec u = s;
    u.name = "r3";
    u.symbols = {"1003"};
    CHECK_FALSE(sm.create(u, err));
    CHECK(err.find("max_pending") != std::string::npos);
    u.recap = false; // a live session is not limited
    REQUIRE_MESSAGE(sm.create(u, err), err);
    // SET_FILTER keeps recap and is held to the same limit.
    t.recap = false;
    t.symbols = {"1002", "1004"}; // 3 distinct with r1
    CHECK_FALSE(sm.set_filter(t, err));
    CHECK(err.find("max_pending") != std::string::npos);
    sm.destroy_all();
}

TEST_CASE("recap merge: pending before the first live message; the floor follows the stream's first sequence") {
    Rig r;
    auto rec = r.app({1001}, true);
    r.requests();
    rec->drain();
    CHECK(r.part.recap->find(r.sym(1001))->floor == 0);
    r.reply(1001, 900, {trade(1001, 0)}, 1); // ahead of live (nothing yet): parked
    CHECK(r.part.recap->parked() == 1);
    r.seq = 999;
    r.live(1001, 1000); // first message seq 1000: floor 999; the parked S = 900 is too old
    auto got = rec->drain();
    REQUIRE(got.size() == 1);
    CHECK(recap_ev(got[0]).status == static_cast<uint8_t>(RecapEventStatus::TOO_OLD));
    CHECK(r.part.recap->parked() == 0);
    CHECK(r.releases().size() == 1);
    REQUIRE(r.requests().size() == 1);
    r.reply(1001, 999, {trade(1001, 0, 5)}, 2);
    got = rec->drain();
    REQUIRE(got.size() == 4);
    CHECK(tag(got[1]) == 5);
    CHECK(got[3].hdr.sequence == 1000);
    CHECK(tag(got[3]) == 1000);
}

TEST_CASE("recap waiters: the last waiter leaving releases a parked reply") {
    Rig r;
    auto rec = r.app({1001}, true);
    r.requests();
    r.reply(1001, 50, {trade(1001, 0)}, 7);
    CHECK(r.part.recap->parked() == 1);
    r.command(PipelineCommand::Type::REMOVE_SESSION, rec->s.get());
    auto rel = r.releases();
    REQUIRE(rel.size() == 1);
    CHECK(rel[0].slab == 7);
    CHECK(r.part.recap->pending() == 0);
    CHECK(r.part.recap->parked() == 0);
}

namespace {
SymbolStaleEvent stale_ev(const Rec& r) { SymbolStaleEvent ev{}; REQUIRE(r.payload.size() == sizeof(ev)); std::memcpy(&ev, r.payload.data(), sizeof(ev)); return ev; }
} // namespace

TEST_CASE("recap triggers: a gap makes every symbol of a recap session pending, renews the rest, drops older replies") {
    Rig r;
    auto live_app = r.app({1001, 1002}, false);
    auto rec = r.app({1001, 1002}, true);
    r.requests();
    rec->drain();
    r.live(1001, 1); // seq 1
    r.reply(1001, 1, {trade(1001, 0, 10)}, 1); // 1001 merged: live from here
    rec->drain();
    CHECK(r.part.recap->pending() == 1); // 1002 still waits
    const uint32_t gen0 = r.part.recap->generation();

    r.seq = 5;
    r.live(1001, 6); // seq 6: 2..5 lost
    CHECK(r.part.recap->generation() == gen0 + 1);
    CHECK(r.part.recap_counters.recap_resyncs.load() == 1);
    auto got = rec->drain();
    REQUIRE(got.size() == 4);
    CHECK(is(got[0], ControlCode::GAP_DETECTED));
    REQUIRE(is(got[1], ControlCode::SYMBOL_STALE));
    CHECK(stale_ev(got[1]).symbol_id == r.sym(1001));
    CHECK(stale_ev(got[1]).reason == static_cast<uint16_t>(SymbolStaleReason::GAP));
    CHECK(stale_ev(got[1]).last_seq == 1);
    CHECK(is(got[2], ControlCode::RECAP_PENDING)); // 1001 was live
    CHECK(recap_ev(got[2]).symbol_id == r.sym(1001));
    REQUIRE(is(got[3], ControlCode::SYMBOL_STALE)); // 1002 was pending already: no second PENDING
    CHECK(stale_ev(got[3]).symbol_id == r.sym(1002));
    auto reqs = r.requests();
    REQUIRE(reqs.size() == 2); // 1002 renewed, 1001 asked for
    for (const auto& c : reqs) CHECK(c.generation == gen0 + 1);
    for (const auto& c : reqs) CHECK(c.bulk);   // a partition-wide resync: one bulk request upstream
    CHECK(r.part.recap->find(r.sym(1001))->floor == 5);
    CHECK(r.part.recap->find(r.sym(1002))->floor == 5);

    // The live session hears of the gap and nothing else, and is not held.
    auto lv = live_app->drain();
    size_t data = 0, ctl = 0;
    for (auto& x : lv) (x.hdr.kind == static_cast<uint8_t>(Kind::DATA) ? data : ctl)++;
    CHECK(data == 2);
    CHECK(ctl == 1);

    // 1002's reply to the old request: dropped unseen, slab released.
    r.reply(1002, 6, {trade(1002, 0)}, 2, 1, gen0);
    CHECK(rec->drain().empty());
    CHECK(r.part.recap_counters.recap_superseded.load() == 1);
    REQUIRE(r.releases().size() == 2);
    CHECK(r.part.recap->pending() == 2);
    // The renewed replies merge; seq 6 is held for 1001 and comes after its snapshot.
    r.reply(1001, 5, {trade(1001, 0, 50)}, 3);
    got = rec->drain();
    REQUIRE(got.size() == 4);
    CHECK(tag(got[1]) == 50);
    CHECK(got[3].hdr.sequence == 6);
    r.reply(1002, 6, {trade(1002, 0, 60)}, 4);
    got = rec->drain();
    REQUIRE(got.size() == 3);
    CHECK(tag(got[1]) == 60);
    CHECK(r.part.recap->pending() == 0);
}

TEST_CASE("recap triggers: a sequence reset drops holds and parked replies, renews, and rebases the floor") {
    Rig r;
    for (int i = 0; i < 2000; ++i) r.live(1001); // seq 2000
    auto rec = r.app({1001}, true);
    r.requests();
    rec->drain();
    r.live(1001); // seq 2001: held
    r.reply(1001, 2500, {trade(1001, 0)}, 1); // ahead of live: parked
    CHECK(r.part.recap->parked() == 1);

    r.seq = 0;
    r.live(1001, 1); // seq 1: back beyond dup_window
    CHECK(r.part.counters.seq_resets.load() == 1);
    auto got = rec->drain();
    REQUIRE(got.size() == 2);
    CHECK(is(got[0], ControlCode::GAP_DETECTED));
    REQUIRE(is(got[1], ControlCode::SYMBOL_STALE));
    CHECK(stale_ev(got[1]).reason == static_cast<uint16_t>(SymbolStaleReason::SEQUENCE_RESET));
    CHECK(stale_ev(got[1]).last_seq == 2001);
    CHECK(r.part.recap->parked() == 0);
    CHECK(r.releases().size() == 1);
    REQUIRE(r.requests().size() == 1);
    // The renewed request carried the old stream's floor; the manager is told the rebased one.
    REQUIRE(r.floors.size() == 1);
    CHECK(r.floors[0].symbol_id == r.sym(1001));
    CHECK(r.floors[0].floor == 0);
    RecapPending* e = r.part.recap->find(r.sym(1001));
    REQUIRE(e);
    CHECK(e->floor == 0);
    CHECK(e->held == 1); // seq 1 of the new stream; 2001 is gone

    r.reply(1001, 0, {trade(1001, 0, 7)}, 2);
    got = rec->drain();
    REQUIRE(got.size() == 4);
    CHECK(tag(got[1]) == 7);
    CHECK(got[3].hdr.sequence == 1);
    CHECK(tag(got[3]) == 1);
}

TEST_CASE("recap triggers: a publisher restart renews at once and parks replies until the new stream's first sequence") {
    Rig r;
    for (int i = 0; i < 10; ++i) r.live(1001); // seq 10
    auto rec = r.app({1001, 1002}, true);
    r.requests();
    r.reply(1001, 10, {trade(1001, 0)}, 1); // 1001 live
    rec->drain();
    const uint32_t gen0 = r.part.recap->generation();

    r.fake->up(TransportReason::PUBLISHER_RESTART);
    r.pipe.run_once();
    auto got = rec->drain();
    REQUIRE(got.size() == 4); // TRANSPORT_UP, STALE 1001, PENDING 1001, STALE 1002
    CHECK(is(got[0], ControlCode::TRANSPORT_UP));
    CHECK(stale_ev(got[1]).reason == static_cast<uint16_t>(SymbolStaleReason::PUBLISHER_RESTART));
    CHECK(is(got[2], ControlCode::RECAP_PENDING));
    CHECK(is(got[3], ControlCode::SYMBOL_STALE));
    CHECK(r.requests().size() == 2);
    CHECK(r.part.recap->awaiting_seq());

    // A reply from before the restart never merges, even one that would fit.
    r.reply(1002, 10, {trade(1002, 0)}, 2, 1, gen0);
    CHECK(rec->drain().empty());
    // A renewed reply before the new stream has a sequence: parked.
    r.reply(1001, 3, {trade(1001, 0, 30)}, 3);
    CHECK(rec->drain().empty());
    CHECK(r.part.recap->parked() == 1);

    // The new publisher starts at 4, with no gap or reset reported against the old stream.
    r.seq = 3;
    r.live(1001, 4);
    CHECK_FALSE(r.part.recap->awaiting_seq());
    CHECK(r.part.counters.gaps.load() == 0);
    CHECK(r.part.counters.seq_resets.load() == 0);
    CHECK(r.requests().empty());
    REQUIRE(r.floors.size() == 2); // the floors follow the new stream, for the manager too
    for (const RecapCommand& c : r.floors) CHECK(c.floor == 3);
    got = rec->drain();
    REQUIRE(got.size() == 4); // the parked S = 3 = floor merges, then seq 4
    CHECK(is(got[0], ControlCode::RECAP_BEGIN));
    CHECK(tag(got[1]) == 30);
    CHECK(got[3].hdr.sequence == 4);
}

TEST_CASE("recap triggers: no recap session, no recap work; a transport bounce alone is no trigger") {
    Rig r;
    auto live_app = r.app({1001}, false);
    r.live(1001);
    r.fake->down(TransportReason::SOURCE_LOST);
    r.fake->up(TransportReason::NONE);
    r.live(1001);
    r.seq = 9;
    r.live(1001); // a gap
    CHECK(r.requests().empty());
    CHECK(r.part.recap->pending() == 0);
    CHECK(r.part.recap_counters.recap_resyncs.load() == 1); // the gap; it found nothing to recap

    auto rec = r.app({1002}, true);
    r.requests();
    r.reply(1002, 10, {trade(1002, 0)}, 1);
    rec->drain();
    const uint32_t gen = r.part.recap->generation();
    r.fake->down(TransportReason::SOURCE_LOST);
    r.fake->up(TransportReason::NONE);
    r.live(1002);
    auto got = rec->drain();
    REQUIRE(got.size() == 3); // DOWN, UP, the trade
    CHECK(got[2].hdr.kind == static_cast<uint8_t>(Kind::DATA));
    CHECK(r.part.recap->generation() == gen);
    CHECK(r.requests().empty());
}

TEST_CASE("recap with instrument sequences: snapshot records carry the instrument's; a jump recaps only that symbol") {
    // MarketSegmentID stands in for an instrument sequence (CME RptSeq) here.
    std::string yaml = partition_yaml();
    yaml.replace(yaml.find("fields: { symbol: SecurityID, sequence: SeqNum }"), 48,
                 "fields: { symbol: SecurityID, sequence: SeqNum, instrument_sequence: MarketSegmentID }");
    Rig r(yaml);
    const SbeSchema& sc = *r.part.schema();
    const FieldDef* iseq_field = sc.find_field(*sc.find_message(1), "MarketSegmentID");
    REQUIRE(iseq_field);
    const auto trade_i = [&](uint64_t sec, uint64_t seq, uint64_t iseq, uint64_t t) {
        std::vector<uint8_t> b = trade(sec, seq, t);
        std::memcpy(b.data() + sc.header().size + iseq_field->offset, &iseq, iseq_field->size);
        return b;
    };
    const auto live = [&](uint64_t sec, uint64_t iseq) {
        ++r.seq;
        r.fake->push(trade_i(sec, r.seq, iseq, r.seq), now_ns());   // tag = the stream sequence
        r.pipe.run_once();
    };
    auto live_app = r.app({1001, 2002}, false);
    live(1001, 1);   // stream 1
    live(1001, 2);   // stream 2
    auto rec = r.app({1001, 2002}, true);
    REQUIRE(r.requests().size() == 2);
    rec->drain();    // RECAP_PENDING x2
    r.reply(2002, 2, {});   // 2002 has no state: an empty snapshot, live from here
    r.reply(1001, 2, {trade_i(1001, 0, 2, 100)});
    auto got = rec->drain();
    REQUIRE(got.size() == 5);   // 2002: BEGIN, COMPLETE; 1001: BEGIN, record, COMPLETE
    size_t i = 0;
    while (i < got.size() && !(is(got[i], ControlCode::RECAP_BEGIN) && recap_ev(got[i]).symbol_id == r.sym(1001))) ++i;
    REQUIRE(i + 2 < got.size());
    CHECK(got[i + 1].hdr.kind == static_cast<uint8_t>(Kind::DATA));
    CHECK(got[i + 1].hdr.sequence == 2);   // the instrument's sequence as of S, not S
    CHECK((got[i + 1].hdr.flags & FLAG_SNAPSHOT_ORIGIN) != 0);
    CHECK(is(got[i + 2], ControlCode::RECAP_COMPLETE));

    live(1001, 3);   // stream 3
    live(2002, 1);   // stream 4
    got = rec->drain();
    REQUIRE(got.size() == 2);
    CHECK(got[0].hdr.sequence == 3);
    CHECK(got[1].hdr.sequence == 1);

    // 1001 jumps 3 -> 5 (the stream did not): only 1001 is recapped.
    live(1001, 5);   // stream 5, held
    live(2002, 2);   // stream 6, live
    live(1001, 6);   // stream 7, held
    got = rec->drain();
    REQUIRE(got.size() == 4);
    CHECK(is(got[0], ControlCode::INSTRUMENT_GAP));
    CHECK(is(got[1], ControlCode::SYMBOL_STALE));
    SymbolStaleEvent se{};
    std::memcpy(&se, got[1].payload.data(), sizeof se);
    CHECK(se.symbol_id == r.sym(1001));
    CHECK(se.reason == static_cast<uint16_t>(SymbolStaleReason::INSTRUMENT_GAP));
    CHECK(is(got[2], ControlCode::RECAP_PENDING));
    CHECK(got[3].hdr.symbol_id == r.sym(2002));
    CHECK(got[3].hdr.sequence == 2);
    const auto reqs = r.requests();
    REQUIRE(reqs.size() == 1);
    CHECK(reqs[0].symbol_id == r.sym(1001));
    CHECK(r.part.counters.snapshot().gaps == 0);
    CHECK(r.part.counters.snapshot().inst_gaps == 1);

    // A snapshot as of stream 5 (instrument 5): the held stream-7 record follows it.
    r.reply(1001, 5, {trade_i(1001, 0, 5, 200)}, 4);
    got = rec->drain();
    REQUIRE(got.size() == 4);
    CHECK(is(got[0], ControlCode::RECAP_BEGIN));
    CHECK(got[1].hdr.sequence == 5);
    CHECK(tag(got[1]) == 200);
    CHECK(is(got[2], ControlCode::RECAP_COMPLETE));
    CHECK(got[3].hdr.sequence == 6);
    CHECK(tag(got[3]) == 7);

    // The live session saw the gap and every record, by instrument sequence.
    std::vector<std::pair<uint32_t, uint64_t>> seen;
    bool gap = false;
    for (auto& x : live_app->drain()) {
        if (is(x, ControlCode::INSTRUMENT_GAP)) gap = true;
        if (x.hdr.kind == static_cast<uint8_t>(Kind::DATA)) seen.emplace_back(x.hdr.symbol_id, x.hdr.sequence);
    }
    CHECK(gap);
    const uint32_t a = r.sym(1001), b = r.sym(2002);
    CHECK(seen == std::vector<std::pair<uint32_t, uint64_t>>{{a, 1}, {a, 2}, {a, 3}, {b, 1}, {a, 5}, {b, 2}, {a, 6}});
}

namespace {

// Two recap partitions of one topic (/1 and /2), in one pipeline, each with its
// own (fake) snapshot service. The test plays both managers.
struct TwoRig {
    SymbolTable symbols{1024};
    Lvc lvc;
    Partition part[2];
    FakeTransport* fake[2] = {};
    Pipeline pipe;
    std::deque<std::vector<uint8_t>> slabs;
    uint64_t seq[2] = {};
    uint32_t next_id = 1;

    TwoRig() : pipe("g0", 4242, symbols, lvc) {
        std::string err;
        REQUIRE(lvc.init(1024, {static_cast<uint8_t>(MsgType::TRADE)}, 256, err));
        for (uint16_t i = 0; i < 2; ++i) {
            PartitionConfig cfg;
            REQUIRE_MESSAGE(PartitionConfig::parse(YAML::Load(partition_yaml()), i, data_dir(), cfg, err), err);
            cfg.set_topic("t", 1, static_cast<uint16_t>(i + 1));
            auto ft = std::make_unique<FakeTransport>(64);
            fake[i] = ft.get();
            REQUIRE_MESSAGE(part[i].build(cfg, err, std::move(ft), std::make_unique<FakeRecap>()), err);
            REQUIRE(part[i].open(err));
            pipe.add_partition(&part[i]);
        }
    }
    uint32_t sym(uint64_t sec) { return symbols.intern_u64(sec); }

    std::unique_ptr<App> app(std::initializer_list<uint64_t> secs) {
        auto a = std::make_unique<App>();
        a->s = std::make_unique<Session>();
        Session& s = *a->s;
        s.id = next_id++;
        s.name = "two" + std::to_string(s.id);
        s.partition_mask = 3;
        s.recap = true;
        FilterSpec fs;
        for (uint64_t sec : secs) fs.symbol_ids.push_back(sym(sec));
        fs.msg_types = {1, 2};
        fs.topic_ids = {1};
        fs.agent_control = true;
        std::string err;
        s.filter_owned = build_filter(fs, 1024, 1, err);
        REQUIRE_MESSAGE(s.filter_owned, err);
        s.filter.store(s.filter_owned.get());
        a->path = "/dev/shm/subplane.recaptest." + std::to_string(getpid()) + "." + s.name + ".ring";
        RingConfig rc;
        rc.capacity_bytes = 1u << 20;
        rc.max_payload = 4096;
        rc.session_name = s.name;
        RingWriter::unlink_ring(a->path);
        REQUIRE_MESSAGE(s.ring.open(rc, a->path, 4242, err), err);
        REQUIRE(a->rd.open(a->path));
        PipelineCommand c;
        c.type = PipelineCommand::Type::ADD_SESSION;
        c.session = &s;
        REQUIRE(pipe.post(c));
        pipe.run_once();
        return a;
    }
    void live(int pi, uint64_t sec) {
        fake[pi]->push(trade(sec, ++seq[pi]), now_ns());
        pipe.run_once();
    }
    // REQUEST and CANCEL; FLOOR updates are set aside in `floors`.
    std::vector<RecapCommand> floors;
    std::vector<RecapCommand> requests() {
        std::vector<RecapCommand> out;
        RecapCommand c;
        while (pipe.recap_channel().requests.pop(c)) (c.op == RecapCommand::Op::FLOOR ? floors : out).push_back(c);
        return out;
    }
    void reply(int pi, uint64_t sec, uint64_t as_of, const std::vector<std::vector<uint8_t>>& records, uint16_t slab) {
        std::vector<uint8_t> buf;
        for (const auto& m : records) { put(buf, m.size(), 4); buf.insert(buf.end(), m.begin(), m.end()); }
        slabs.push_back(std::move(buf));
        RecapReply r;
        r.partition = static_cast<uint16_t>(pi);
        r.slab = slab;
        r.symbol_id = sym(sec);
        r.request_id = 900 + slab;
        r.attempt = 1;
        r.generation = part[pi].recap->generation();
        r.status = RecapStatus::OK;
        r.as_of = as_of;
        r.data = slabs.back().data();
        r.bytes = static_cast<uint32_t>(slabs.back().size());
        r.messages = static_cast<uint32_t>(records.size());
        REQUIRE(pipe.recap_channel().replies.push(r));
        pipe.run_once();
    }
    void unknown(int pi, uint64_t sec) {
        RecapReply r;
        r.partition = static_cast<uint16_t>(pi);
        r.symbol_id = sym(sec);
        r.request_id = 77;
        r.attempt = 1;
        r.generation = part[pi].recap->generation();
        r.status = RecapStatus::REJECTED;
        r.reason = static_cast<uint64_t>(sp_proto::RejectReason::UnknownInstrument);
        REQUIRE(pipe.recap_channel().replies.push(r));
        pipe.run_once();
    }
};

size_t count(const std::vector<Rec>& v, ControlCode c) {
    size_t n = 0;
    for (const auto& r : v) n += is(r, c) ? 1u : 0u;
    return n;
}

} // namespace

TEST_CASE("recap across partitions: an unseen symbol is asked of each; the one that has it serves it, once") {
    TwoRig r;
    auto app = r.app({1001, 1002});
    auto reqs = r.requests();
    REQUIRE(reqs.size() == 4);   // both symbols of both partitions: neither has been seen
    auto got = app->drain();
    CHECK(count(got, ControlCode::RECAP_PENDING) == 2);   // one per symbol, not per partition

    // /2 does not have 1001: quiet, /1 still tries. /1 serves it and /2 stops asking.
    r.unknown(1, 1001);
    CHECK(app->drain().empty());
    CHECK(r.part[1].recap_counters.recap_elsewhere.load() == 1);
    r.reply(0, 1001, 0, {trade(1001, 0)}, 1);
    got = app->drain();
    CHECK(count(got, ControlCode::RECAP_COMPLETE) == 1);
    CHECK(r.part[1].recap->find(r.sym(1001)) == nullptr);
    reqs = r.requests();
    REQUIRE(reqs.size() == 1);
    CHECK(reqs[0].op == RecapCommand::Op::CANCEL);
    CHECK(reqs[0].partition == 1);

    // /1 merges 1002 first: /2's request is cancelled before it answers.
    r.reply(1, 1002, 0, {trade(1002, 0)}, 2);
    got = app->drain();
    CHECK(count(got, ControlCode::RECAP_COMPLETE) == 1);
    CHECK(count(got, ControlCode::RECAP_FAILED) == 0);
    CHECK(r.part[0].recap->pending() == 0);
    CHECK(r.part[1].recap->pending() == 0);
}

TEST_CASE("recap across partitions: unknown on every partition is a failure") {
    TwoRig r;
    auto app = r.app({1001});
    r.requests();
    app->drain();
    r.unknown(0, 1001);
    CHECK(app->drain().empty());
    r.unknown(1, 1001);
    auto got = app->drain();
    REQUIRE(count(got, ControlCode::RECAP_FAILED) == 1);
    CHECK(r.part[0].recap->find(r.sym(1001)) != nullptr);   // still waiting: the managers retry
}

TEST_CASE("recap across partitions: a seen symbol is asked only of its partition; a gap there stales only its own") {
    TwoRig r;
    r.live(0, 1001);
    r.live(1, 2002);
    auto app = r.app({1001, 2002});
    auto reqs = r.requests();
    REQUIRE(reqs.size() == 2);
    for (const auto& c : reqs) CHECK(c.partition == (c.symbol_id == r.sym(1001) ? 0 : 1));
    r.reply(0, 1001, 1, {trade(1001, 0)}, 1);
    r.reply(1, 2002, 1, {trade(2002, 0)}, 2);
    app->drain();

    // /2 loses 2..4: 2002 is stale and asked for in one bulk on /2; 1001 is not touched.
    r.seq[1] = 4;
    r.live(1, 2002);
    auto got = app->drain();
    REQUIRE(count(got, ControlCode::SYMBOL_STALE) == 1);
    SymbolStaleEvent se{};
    for (const auto& x : got) if (is(x, ControlCode::SYMBOL_STALE)) std::memcpy(&se, x.payload.data(), sizeof se);
    CHECK(se.symbol_id == r.sym(2002));
    reqs = r.requests();
    REQUIRE(reqs.size() == 1);
    CHECK(reqs[0].partition == 1);
    CHECK(reqs[0].symbol_id == r.sym(2002));
    CHECK(reqs[0].bulk);
    CHECK(r.part[0].recap->pending() == 0);
    // 1001 kept flowing live the whole time.
    r.live(0, 1001);
    got = app->drain();
    REQUIRE(got.size() == 1);
    CHECK(got[0].hdr.symbol_id == r.sym(1001));
}
