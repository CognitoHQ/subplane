#include <doctest.h>

#include "fake_recap.h"
#include "fake_transport.h"
#include "subplane/pipeline/partition.h"
#include "subplane/recap/recap_manager.h"
#include "subplane/symbol_table.h"

#include <cstring>
#include <string>
#include <vector>

using namespace subplane;
using subplane::test::FakeRecap;
using subplane::test::FakeTransport;

namespace {

std::string data_dir() {
    std::string f = __FILE__;
    return f.substr(0, f.find_last_of('/')) + "/../data";
}

constexpr uint64_t kMs = 1'000'000ull;
constexpr uint64_t kEpoch = 777;

// Sample schema roles: SeqReset (101) is RECAP_BEGIN, Heartbeat (100) is
// RECAP_END, Ticker (4) is RECAP_REJECT, Trade (1) is a reply record.
std::string partition_yaml(const std::string& extra_decoder = "", int max_inflight = 4) {
    return R"(
name: p0
transport: { type: udp_multicast, group: "", port: 1 }
framing: { sequence: message.SeqNum }
decoder:
  type: sbe_routing
  schema: sample_md.xml
  fields: { symbol: SecurityID, sequence: SeqNum, venue: MarketSegmentID }
  symbol_id_mode: table
)" + extra_decoder + R"(
  messages:
    - { template: 1,   msg_type: TRADE }
    - { template: 101, admin: RECAP_BEGIN,  fields: { request_id: SeqNum, as_of: NewSeq } }
    - { template: 100, admin: RECAP_END,    fields: { request_id: SeqNum } }
    - { template: 4,   admin: RECAP_REJECT, fields: { request_id: SeqNum, reason: TransactTime } }
recap:
  type: aeron
  timeout_ms: 100
  max_inflight: )" + std::to_string(max_inflight) + R"(
  retry: { base_ms: 10, max_ms: 40 }
  reply_slabs: 2
  reply_slab_bytes: 4096
)";
}

void put(std::vector<uint8_t>& b, uint64_t v, unsigned size) {
    for (unsigned i = 0; i < size; ++i) b.push_back(static_cast<uint8_t>(v >> (8 * i)));
}
void header(std::vector<uint8_t>& b, uint16_t blk, uint16_t tid) { put(b, blk, 2); put(b, tid, 2); put(b, 7, 2); put(b, 2, 2); }

std::vector<uint8_t> begin_msg(uint64_t rid, uint64_t as_of) { std::vector<uint8_t> b; header(b, 16, 101); put(b, rid, 8); put(b, as_of, 8); return b; }
std::vector<uint8_t> end_msg(uint64_t rid) { std::vector<uint8_t> b; header(b, 8, 100); put(b, rid, 8); return b; }
std::vector<uint8_t> reject_msg(uint64_t rid, uint64_t reason) {
    std::vector<uint8_t> b; header(b, 24, 4);
    b.insert(b.end(), 8, 0);  // Symbol
    put(b, rid, 8);           // SeqNum
    put(b, reason, 8);        // TransactTime
    return b;
}
std::vector<uint8_t> trade_msg(uint64_t sec, uint64_t seq) {
    std::vector<uint8_t> b; header(b, 39, 1);
    put(b, sec, 8); put(b, seq, 8); put(b, 0, 8); put(b, 0, 2);
    b.insert(b.end(), 39 - 26, 0);
    return b;
}
std::vector<uint8_t> cat(std::initializer_list<std::vector<uint8_t>> parts) {
    std::vector<uint8_t> out;
    for (const auto& p : parts) out.insert(out.end(), p.begin(), p.end());
    return out;
}

struct Rig {
    SymbolTable symbols{1024};
    Partition part;
    FakeRecap* recap = nullptr;
    RecapChannel ch;
    RecapManager mgr{kEpoch, symbols};
    uint64_t now = 1000 * kMs;

    explicit Rig(const std::string& yaml = partition_yaml()) {
        std::string err;
        PartitionConfig cfg;
        REQUIRE_MESSAGE(PartitionConfig::parse(YAML::Load(yaml), 0, data_dir(), cfg, err), err);
        auto fr = std::make_unique<FakeRecap>();
        recap = fr.get();
        REQUIRE_MESSAGE(part.build(cfg, err, std::make_unique<FakeTransport>(), std::move(fr)), err);
        REQUIRE(part.has_recap());
        REQUIRE_MESSAGE(mgr.add_partition(part, ch, err), err);
        REQUIRE_MESSAGE(mgr.open(err), err);
    }
    void request(uint32_t sym) { REQUIRE(ch.requests.push({RecapCommand::Op::REQUEST, 0, sym})); }
    void cancel(uint32_t sym) { REQUIRE(ch.requests.push({RecapCommand::Op::CANCEL, 0, sym})); }
    void step(uint64_t advance_ms = 0) { now += advance_ms * kMs; mgr.run_once(now); }
    std::vector<RecapReply> replies() {
        std::vector<RecapReply> out;
        RecapReply r;
        while (ch.replies.pop(r)) out.push_back(r);
        return out;
    }
    const RecapCounters& c() const { return part.recap_counters; }
};

// Splits an OK reply's slab into its messages.
std::vector<std::vector<uint8_t>> records(const RecapReply& r) {
    std::vector<std::vector<uint8_t>> out;
    uint32_t at = 0;
    for (uint32_t i = 0; i < r.messages; ++i) {
        uint32_t len;
        std::memcpy(&len, r.data + at, 4);
        out.emplace_back(r.data + at + 4, r.data + at + 4 + len);
        at += 4 + len;
    }
    CHECK(at == r.bytes);
    return out;
}

} // namespace

TEST_CASE("recap request: 64 bytes, SPRQ, fields where the contract says") {
    RecapRequestWire w;
    w.request_id = 0x1122334455667788ull;
    w.instrument = 42;
    w.venue = 9;
    w.last_seq_seen = 1000;
    uint8_t b[64];
    std::memcpy(b, &w, 64);
    CHECK(std::memcmp(b, "SPRQ", 4) == 0);
    CHECK(b[4] == 1);
    CHECK(b[8] == 0x88);
    CHECK(b[24] == 42);
    CHECK(b[32] == 9);
    uint64_t v;
    std::memcpy(&v, b + 40, 8);
    CHECK(v == 1000);
}

TEST_CASE("recap manager: one request per symbol however many ask; the reply assembles across fragments") {
    Rig r;
    const uint32_t sym = r.symbols.intern_u64(4242);
    r.part.last_seq_pub.store(555);
    r.request(sym);
    r.request(sym);
    r.step();
    r.request(sym); // while in flight
    r.step();
    REQUIRE(r.recap->sent.size() == 1);
    const RecapRequestWire& w = r.recap->sent[0];
    CHECK(w.magic == kRecapRequestMagic);
    CHECK(w.version == 1);
    CHECK(w.request_id == 1);
    CHECK(w.agent_id == kEpoch);
    CHECK(w.instrument == 4242);
    CHECK(w.venue == 0);
    CHECK(w.last_seq_seen == 555);
    CHECK(r.c().recap_requests == 1);
    CHECK(r.c().recap_coalesced == 2);
    CHECK(r.c().inflight == 1);

    const auto t1 = trade_msg(4242, 1), t2 = trade_msg(4242, 2);
    r.recap->push(cat({begin_msg(1, 5000), t1}));
    r.recap->push(t2);
    r.recap->push(end_msg(1));
    r.step();
    auto rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].status == RecapStatus::OK);
    CHECK(rs[0].symbol_id == sym);
    CHECK(rs[0].request_id == 1);
    CHECK(rs[0].as_of == 5000);
    CHECK_FALSE(rs[0].has_source_epoch);
    REQUIRE(rs[0].messages == 2);
    auto recs = records(rs[0]);
    CHECK(recs[0] == t1);
    CHECK(recs[1] == t2);
    CHECK(r.c().recap_replies == 1);
    CHECK(r.c().inflight == 0);
    CHECK(r.c().queued == 0);

    // The symbol is done: a new request is a new upstream request.
    r.request(sym);
    r.step();
    REQUIRE(r.recap->sent.size() == 2);
    CHECK(r.recap->sent[1].request_id == 2);
}

TEST_CASE("recap manager: max_inflight queues the rest in order") {
    Rig r(partition_yaml("", 1));
    const uint32_t a = r.symbols.intern_u64(1), b = r.symbols.intern_u64(2);
    r.request(a);
    r.request(b);
    r.step();
    REQUIRE(r.recap->sent.size() == 1);
    CHECK(r.recap->sent[0].instrument == 1);
    CHECK(r.c().queued == 1);
    r.recap->push(cat({begin_msg(1, 10), end_msg(1)}));
    r.step();
    REQUIRE(r.recap->sent.size() == 2);
    CHECK(r.recap->sent[1].instrument == 2);
    auto rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].messages == 0);
}

TEST_CASE("recap manager: timeout reports, backs off, retries with a new id; cancel stops it") {
    Rig r;
    const uint32_t sym = r.symbols.intern_u64(7);
    r.request(sym);
    r.step();
    REQUIRE(r.recap->sent.size() == 1);
    r.step(99);
    CHECK(r.replies().empty());
    r.step(1); // 100 ms: timed out
    auto rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].status == RecapStatus::TIMEOUT);
    CHECK(rs[0].request_id == 1);
    CHECK(r.c().recap_timeouts == 1);
    // First retry after base/2 .. base (10 ms): not before 5 ms, by 10 ms.
    r.step(4);
    CHECK(r.recap->sent.size() == 1);
    r.step(6);
    REQUIRE(r.recap->sent.size() == 2);
    CHECK(r.recap->sent[1].request_id == 2);

    // A late reply to the first request is stray, not a success.
    r.recap->push(cat({begin_msg(1, 10), trade_msg(7, 1), end_msg(1)}));
    r.step();
    CHECK(r.replies().empty());
    CHECK(r.c().recap_stray == 1); // the unknown BEGIN; its record and END are skipped quietly

    // Cancel while in flight: its timeout is silent and nothing is retried.
    r.cancel(sym);
    r.step(100);
    CHECK(r.replies().empty());
    r.step(100);
    CHECK(r.recap->sent.size() == 2);
    CHECK(r.c().recap_cancelled == 1);
    CHECK(r.c().inflight == 0);
    CHECK(r.c().queued == 0);
}

TEST_CASE("recap manager: backoff grows to max_ms and stays within [cap/2, cap]") {
    Rig r;
    const uint32_t sym = r.symbols.intern_u64(7);
    r.recap->send_status = RecapSendStatus::NOT_CONNECTED;
    r.request(sym);
    r.step();
    auto rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].status == RecapStatus::SEND_FAILED);
    // Attempts 1..6: caps 10, 20, 40, 40, 40, 40 ms. Step 1 ms at a time and
    // measure the gap between failures.
    uint64_t last = r.now;
    const uint64_t caps[] = {10, 20, 40, 40, 40, 40};
    for (uint64_t cap : caps) {
        int guard = 0;
        while (r.replies().empty() && ++guard < 200) r.step(1);
        const uint64_t gap_ms = (r.now - last) / kMs;
        CHECK(gap_ms >= cap / 2);
        CHECK(gap_ms <= cap + 1);
        last = r.now;
    }
    CHECK(r.c().recap_send_failed == 7);
    CHECK(r.recap->sent.empty());
}

TEST_CASE("recap manager: reject reports the reason and retries") {
    Rig r;
    const uint32_t sym = r.symbols.intern_u64(7);
    r.request(sym);
    r.step();
    r.recap->push(reject_msg(1, 3));
    r.step();
    auto rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].status == RecapStatus::REJECTED);
    CHECK(rs[0].reason == 3);
    CHECK(r.c().recap_rejects == 1);
    r.step(40);
    CHECK(r.recap->sent.size() == 2);
}

TEST_CASE("recap manager: stray records and unknown requests are counted, not delivered") {
    Rig r;
    const uint32_t sym = r.symbols.intern_u64(7);
    r.request(sym);
    r.step();
    r.recap->push(trade_msg(7, 1));                                    // outside any reply
    r.recap->push(cat({begin_msg(99, 1), trade_msg(7, 2), end_msg(99)})); // nobody asked
    r.recap->push(cat({begin_msg(1, 20), trade_msg(7, 3), end_msg(1)}));
    r.step();
    auto rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].request_id == 1);
    CHECK(rs[0].messages == 1);
    CHECK(r.c().recap_stray == 2); // the lone trade and the unknown BEGIN; its records and END are skipped quietly
}

TEST_CASE("recap manager: a reply larger than the slab is an overflow; slabs are reused after release") {
    Rig r;
    const uint32_t a = r.symbols.intern_u64(1), b = r.symbols.intern_u64(2), c = r.symbols.intern_u64(3);
    r.request(a);
    r.step();
    std::vector<uint8_t> big = begin_msg(1, 1);
    for (int i = 0; i < 100; ++i) { auto t = trade_msg(1, static_cast<uint64_t>(i)); big.insert(big.end(), t.begin(), t.end()); } // 100 * 51 > 4096
    auto e = end_msg(1);
    big.insert(big.end(), e.begin(), e.end());
    r.recap->push(big);
    r.step();
    auto rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].status == RecapStatus::OVERFLOW);
    CHECK(r.c().recap_overflows == 1);

    // Two slabs: two OK replies held by the pipeline use both; a third reply
    // finds none and times out, then succeeds once a slab comes back.
    r.request(b);
    r.request(c);
    r.step(40); // b and c go out, then a's retry
    REQUIRE(r.recap->sent.size() == 4);
    auto id_of = [&](uint64_t instrument) {
        for (auto it = r.recap->sent.rbegin(); it != r.recap->sent.rend(); ++it) if (it->instrument == instrument) return it->request_id;
        return uint64_t{0};
    };
    r.recap->push(cat({begin_msg(id_of(2), 1), end_msg(id_of(2))}));
    r.recap->push(cat({begin_msg(id_of(3), 1), end_msg(id_of(3))}));
    r.recap->push(cat({begin_msg(id_of(1), 1), end_msg(id_of(1))}));
    r.step();
    rs = r.replies();
    REQUIRE(rs.size() == 2);
    CHECK(rs[0].slab != rs[1].slab);
    CHECK(r.c().recap_no_slab == 1);
    REQUIRE(r.ch.releases.push({0, rs[0].slab}));
    r.step(100); // a's request timed out
    rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].status == RecapStatus::TIMEOUT);
    r.step(40);
    REQUIRE(r.recap->sent.size() == 5);
    r.recap->push(cat({begin_msg(r.recap->sent.back().request_id, 1), end_msg(r.recap->sent.back().request_id)}));
    r.step();
    rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].status == RecapStatus::OK);
    CHECK(rs[0].symbol_id == a);
}

TEST_CASE("recap manager: venue-scoped symbols send the wire instrument and venue") {
    Rig r(partition_yaml("  symbol_scope: venue\n"));
    const uint32_t sym = r.symbols.intern_u64(55, 10);
    r.request(sym);
    r.step();
    REQUIRE(r.recap->sent.size() == 1);
    CHECK(r.recap->sent[0].instrument == 55);
    CHECK(r.recap->sent[0].venue == 10);
}

TEST_CASE("recap config: what a partition needs before it can recap") {
    auto build = [](const std::string& yaml, std::string& err) {
        PartitionConfig cfg;
        if (!PartitionConfig::parse(YAML::Load(yaml), 0, data_dir(), cfg, err)) return false;
        Partition p;
        return p.build(cfg, err, std::make_unique<FakeTransport>());
    };
    std::string err;
    // Markers missing.
    std::string y = partition_yaml();
    y.erase(y.find("    - { template: 100, admin: RECAP_END"), std::string("    - { template: 100, admin: RECAP_END,    fields: { request_id: SeqNum } }\n").size());
    CHECK_FALSE(build(y, err));
    CHECK_MESSAGE(err.find("must map RECAP_BEGIN and RECAP_END") != std::string::npos, err);
    // String symbol.
    y = partition_yaml();
    y.replace(y.find("symbol: SecurityID"), 18, "symbol: Symbol");
    CHECK_FALSE(build(y, err));
    CHECK_MESSAGE(err.find("integer symbol") != std::string::npos, err);
    // Unknown component type lists what exists.
    y = partition_yaml();
    y.replace(y.find("type: aeron"), 11, "type: carrier_pigeon");
    CHECK_FALSE(build(y, err));
    CHECK_MESSAGE(err.find("unknown component type 'carrier_pigeon'") != std::string::npos, err);
    // Manager keys.
    y = partition_yaml();
    y.replace(y.find("reply_slabs: 2"), 14, "reply_slabs: 0");
    CHECK_FALSE(build(y, err));
    CHECK_MESSAGE(err.find("reply_slabs") != std::string::npos, err);
    // dbn cannot recap.
    CHECK_FALSE(build(R"(
name: d
transport: { type: udp, group: "", port: 1 }
decoder: { type: dbn }
recap: { type: aeron }
)", err));
    CHECK_MESSAGE(err.find("dbn cannot recap") != std::string::npos, err);
#if SUBPLANE_WITH_AERON
    // aeron component keys are checked when the component is built.
    CHECK_FALSE(build(partition_yaml(), err));
    CHECK_MESSAGE(err.find("recap.request must be") != std::string::npos, err);
#endif
}
