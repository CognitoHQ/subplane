#include <doctest.h>

#include "fake_recap.h"
#include "fake_transport.h"
#include "subplane/pipeline/partition.h"
#include "subplane/proto/subplane_proto_codec.h"
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

// Replies are SPP unless `reply: stream_markers`. For the marker mode, the
// sample schema roles: SeqReset (101) is RECAP_BEGIN, Heartbeat (100) is
// RECAP_END, Ticker (4) is RECAP_REJECT, Trade (1) is a reply record.
std::string partition_yaml(const std::string& extra_decoder = "", int max_inflight = 4, const std::string& extra_recap = "") {
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
  coalesce_ms: 0
  reply_slabs: 2
  reply_slab_bytes: 4096
)" + extra_recap;
}
const std::string kMarkers = "  reply: stream_markers\n";

void put(std::vector<uint8_t>& b, uint64_t v, unsigned size) {
    for (unsigned i = 0; i < size; ++i) b.push_back(static_cast<uint8_t>(v >> (8 * i)));
}
void header(std::vector<uint8_t>& b, uint16_t blk, uint16_t tid) { put(b, blk, 2); put(b, tid, 2); put(b, 7, 2); put(b, 2, 2); }

// Stream-schema markers (reply: stream_markers).
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

// SPP messages (reply: spp), each with its u32 length prefix.
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
std::vector<uint8_t> s_begin(uint64_t rid, uint64_t as_of, uint64_t epoch = 0) {
    return spp<sp_proto::RecapBeginEncoder>([&](auto& e) { e.CorrelationId(rid).AsOf(as_of).SourceEpoch(epoch); });
}
std::vector<uint8_t> s_record(uint64_t rid, const std::vector<uint8_t>& payload) {
    return spp<sp_proto::RecapRecordEncoder>([&](auto& e) { e.CorrelationId(rid).Payload(payload.data(), payload.size()); });
}
std::vector<uint8_t> s_end(uint64_t rid, uint32_t count) {
    return spp<sp_proto::RecapEndEncoder>([&](auto& e) { e.CorrelationId(rid).RecordCount(count); });
}
std::vector<uint8_t> s_reject(uint64_t rid, sp_proto::RejectReason why) {
    return spp<sp_proto::RecapRejectEncoder>([&](auto& e) { e.CorrelationId(rid).Reason(why); });
}
std::vector<uint8_t> s_status(uint64_t cid, sp_proto::StatusCode code) {
    return spp<sp_proto::StatusEncoder>([&](auto& e) { e.CorrelationId(cid).Code(code); });
}
std::vector<uint8_t> s_hello(uint16_t min_v, uint16_t max_v, const char* name) {
    return spp<sp_proto::HelloEncoder>([&](auto& e) {
        e.CorrelationId(1).PeerEpoch(99).ProtoVersion(max_v).MinProtoVersion(min_v).Role(sp_proto::Role::Snap).ServiceName(name);
    });
}
// A full SPP reply carrying `payloads` as records.
std::vector<uint8_t> s_reply(uint64_t rid, uint64_t as_of, std::initializer_list<std::vector<uint8_t>> payloads) {
    std::vector<uint8_t> out = s_begin(rid, as_of);
    uint32_t n = 0;
    for (const auto& p : payloads) { auto r = s_record(rid, p); out.insert(out.end(), r.begin(), r.end()); ++n; }
    auto e = s_end(rid, n);
    out.insert(out.end(), e.begin(), e.end());
    return out;
}

// What the manager sent, decoded: every SPP message of every frame.
struct Sent {
    uint16_t template_id = 0;
    std::vector<uint8_t> msg;
};
std::vector<Sent> sent_messages(const FakeRecap& f) {
    std::vector<Sent> out;
    for (const auto& fr : f.sent) {
        uint32_t at = 0;
        while (at + 4 <= fr.size()) {
            uint32_t n;
            std::memcpy(&n, fr.data() + at, 4);
            REQUIRE(at + 4 + n <= fr.size());
            sp_proto::Header h;
            REQUIRE(sp_proto::read_header(fr.data() + at + 4, n, h));
            CHECK(h.schema_id == sp_proto::kSchemaId);
            out.push_back({h.template_id, std::vector<uint8_t>(fr.data() + at + 4, fr.data() + at + 4 + n)});
            at += 4 + n;
        }
        CHECK(at == fr.size());
    }
    return out;
}
struct Req {
    uint64_t id, agent_epoch, instrument, last_seq;
    uint16_t venue;
};
std::vector<Req> requests(const FakeRecap& f) {
    std::vector<Req> out;
    for (const auto& m : sent_messages(f)) {
        if (m.template_id != sp_proto::RecapRequestDecoder::kTemplateId) continue;
        sp_proto::RecapRequestDecoder d;
        REQUIRE(d.wrap(m.msg.data(), m.msg.size()));
        out.push_back({d.CorrelationId(), d.AgentEpoch(), d.Instrument(), d.LastSeqSeen(), d.Venue()});
    }
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
    void request(uint32_t sym, uint32_t generation = 0) { REQUIRE(ch.requests.push({RecapCommand::Op::REQUEST, 0, sym, generation})); }
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

TEST_CASE("recap manager: Hello first, then one RecapRequest per symbol however many ask; the reply assembles across fragments") {
    Rig r;
    const uint32_t sym = r.symbols.intern_u64(4242);
    r.part.last_seq_pub.store(555);
    r.request(sym);
    r.request(sym);
    r.step();
    r.request(sym); // while in flight
    r.step();
    const auto msgs = sent_messages(*r.recap);
    REQUIRE(msgs.size() == 2);
    REQUIRE(msgs[0].template_id == sp_proto::HelloDecoder::kTemplateId);
    sp_proto::HelloDecoder hello;
    REQUIRE(hello.wrap(msgs[0].msg.data(), msgs[0].msg.size()));
    CHECK(hello.Role() == sp_proto::Role::Agent);
    CHECK(hello.PeerEpoch() == kEpoch);
    CHECK(hello.ProtoVersion() == sp_proto::kSchemaVersion);
    CHECK(hello.MinProtoVersion() == 1);
    CHECK(hello.ServiceName() == "subplaned");
    CHECK(hello.Extensions_count() == 0);
    const auto rq = requests(*r.recap);
    REQUIRE(rq.size() == 1);
    CHECK(rq[0].agent_epoch == kEpoch);
    CHECK(rq[0].instrument == 4242);
    CHECK(rq[0].venue == 0);
    CHECK(rq[0].last_seq == 555);
    CHECK(r.c().recap_requests == 1);
    CHECK(r.c().recap_coalesced == 2);
    CHECK(r.c().inflight == 1);

    const uint64_t id = rq[0].id;
    const auto t1 = trade_msg(4242, 1), t2 = trade_msg(4242, 2);
    r.recap->push(cat({s_begin(id, 5000), s_record(id, t1)}));
    r.recap->push(s_record(id, t2));
    r.recap->push(s_end(id, 2));
    r.step();
    auto rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].status == RecapStatus::OK);
    CHECK(rs[0].symbol_id == sym);
    CHECK(rs[0].request_id == id);
    CHECK(rs[0].as_of == 5000);
    CHECK_FALSE(rs[0].has_source_epoch);
    REQUIRE(rs[0].messages == 2);
    auto recs = records(rs[0]);
    CHECK(recs[0] == t1);
    CHECK(recs[1] == t2);
    CHECK(r.c().recap_replies == 1);
    CHECK(r.c().inflight == 0);
    CHECK(r.c().queued == 0);

    // The symbol is done: a new request is a new upstream request, and the
    // Hello is not repeated on the same connection.
    r.request(sym);
    r.step();
    REQUIRE(requests(*r.recap).size() == 2);
    CHECK(requests(*r.recap)[1].id > id);
    CHECK(sent_messages(*r.recap).size() == 3);
}

TEST_CASE("recap manager: coalesce_ms gathers the sessions that ask within it into one request") {
    std::string y = partition_yaml();
    y.replace(y.find("coalesce_ms: 0"), 14, "coalesce_ms: 50");
    Rig r(y);
    const uint32_t a = r.symbols.intern_u64(1), b = r.symbols.intern_u64(2);
    r.request(a);
    r.step();
    CHECK(requests(*r.recap).empty());   // gathering
    r.request(a);
    r.step(30);
    r.request(b);                        // its own window, from when the manager sees it
    r.request(a);
    r.step();
    r.step(19);
    CHECK(requests(*r.recap).empty());
    r.step(1);                           // a: 50 ms since its first request
    auto rq = requests(*r.recap);
    REQUIRE(rq.size() == 1);
    CHECK(rq[0].instrument == 1);
    CHECK(r.c().recap_coalesced == 2);
    r.step(29);
    CHECK(requests(*r.recap).size() == 1);
    r.step(1);                           // b: 50 ms since its first request
    rq = requests(*r.recap);
    REQUIRE(rq.size() == 2);
    CHECK(rq[1].instrument == 2);
    CHECK(r.c().recap_requests == 2);
    // A renew does not gather: the symbol has waited already.
    r.request(a, 1);
    r.step();
    CHECK(requests(*r.recap).size() == 3);
}

TEST_CASE("recap manager: records are opaque payloads; a source epoch comes through") {
    Rig r;
    const uint32_t sym = r.symbols.intern_u64(9);
    r.request(sym);
    r.step();
    const uint64_t id = requests(*r.recap)[0].id;
    const std::vector<uint8_t> odd = {1, 2, 3}; // not a message of the stream's schema: the manager never looks
    r.recap->push(cat({s_begin(id, 70, 0xABCD), s_record(id, odd), s_record(id, {}), s_end(id, 2)}));
    r.step();
    auto rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].has_source_epoch);
    CHECK(rs[0].source_epoch == 0xABCD);
    REQUIRE(rs[0].messages == 2);
    auto recs = records(rs[0]);
    CHECK(recs[0] == odd);
    CHECK(recs[1].empty());
}

TEST_CASE("recap manager: max_inflight queues the rest in order") {
    Rig r(partition_yaml("", 1));
    const uint32_t a = r.symbols.intern_u64(1), b = r.symbols.intern_u64(2);
    r.request(a);
    r.request(b);
    r.step();
    REQUIRE(requests(*r.recap).size() == 1);
    CHECK(requests(*r.recap)[0].instrument == 1);
    CHECK(r.c().queued == 1);
    r.recap->push(s_reply(requests(*r.recap)[0].id, 10, {}));
    r.step();
    REQUIRE(requests(*r.recap).size() == 2);
    CHECK(requests(*r.recap)[1].instrument == 2);
    auto rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].messages == 0);
}

TEST_CASE("recap manager: more requests time out at once than one batch of 64; every one is reported") {
    Rig r(partition_yaml("", 100));
    for (uint64_t i = 1; i <= 70; ++i) r.request(r.symbols.intern_u64(i));
    for (int k = 0; k < 4 && requests(*r.recap).size() < 70; ++k) r.step();
    REQUIRE(requests(*r.recap).size() == 70);
    CHECK(r.replies().empty());
    r.step(100);
    const auto rs = r.replies();
    CHECK(rs.size() == 70);
    for (const auto& x : rs) CHECK(x.status == RecapStatus::TIMEOUT);
    CHECK(r.c().recap_timeouts == 70);
    CHECK(r.c().inflight == 0);
}

TEST_CASE("recap manager: timeout reports, backs off, retries with a new id; cancel stops it") {
    Rig r;
    const uint32_t sym = r.symbols.intern_u64(7);
    r.request(sym);
    r.step();
    REQUIRE(requests(*r.recap).size() == 1);
    const uint64_t first = requests(*r.recap)[0].id;
    r.step(99);
    CHECK(r.replies().empty());
    r.step(1); // 100 ms: timed out
    auto rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].status == RecapStatus::TIMEOUT);
    CHECK(rs[0].request_id == first);
    CHECK(r.c().recap_timeouts == 1);
    // First retry after base/2 .. base (10 ms): not before 5 ms, by 10 ms.
    r.step(4);
    CHECK(requests(*r.recap).size() == 1);
    r.step(6);
    REQUIRE(requests(*r.recap).size() == 2);
    CHECK(requests(*r.recap)[1].id != first);

    // A late reply to the first request is stray, not a success.
    r.recap->push(s_reply(first, 10, {trade_msg(7, 1)}));
    r.step();
    CHECK(r.replies().empty());
    CHECK(r.c().recap_stray == 1); // the unknown RecapBegin; its record and end are skipped quietly

    // Cancel while in flight: its timeout is silent and nothing is retried.
    r.cancel(sym);
    r.step(100);
    CHECK(r.replies().empty());
    r.step(100);
    CHECK(requests(*r.recap).size() == 2);
    CHECK(r.c().recap_cancelled == 1);
    CHECK(r.c().inflight == 0);
    CHECK(r.c().queued == 0);
}

TEST_CASE("recap manager: a request with a newer generation renews the symbol at once; the old reply is stray") {
    Rig r;
    const uint32_t sym = r.symbols.intern_u64(7);
    r.request(sym);
    r.step();
    const uint64_t first = requests(*r.recap)[0].id;
    // The first reply is half assembled when the renew arrives.
    r.recap->push(cat({s_begin(first, 10), s_record(first, trade_msg(7, 1))}));
    r.step();
    r.request(sym, 1);
    r.step();
    REQUIRE(requests(*r.recap).size() == 2);
    const uint64_t second = requests(*r.recap)[1].id;
    CHECK(second != first);
    CHECK(r.c().recap_renewed == 1);
    CHECK(r.c().recap_coalesced == 0);
    CHECK(r.c().inflight == 1);
    r.recap->push(s_end(first, 1)); // the rest of the abandoned reply is skipped
    r.recap->push(s_reply(second, 20, {trade_msg(7, 2)}));
    r.step();
    auto rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].request_id == second);
    CHECK(rs[0].generation == 1);
    CHECK(rs[0].attempt == 1);
    CHECK(rs[0].as_of == 20);
    r.mgr.run_once(r.now); // nothing else arrives
    CHECK(r.replies().empty());
    for (const auto& x : rs) REQUIRE(r.ch.releases.push({0, x.slab}));

    // Same generation coalesces; a renew in backoff goes out without waiting
    // for it, with the attempt count reset.
    r.request(sym, 1);
    r.step();
    REQUIRE(requests(*r.recap).size() == 3);
    r.request(sym, 1);
    r.step();
    CHECK(r.c().recap_coalesced == 1);
    r.step(100); // timed out: backoff
    rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].status == RecapStatus::TIMEOUT);
    CHECK(rs[0].generation == 1);
    r.request(sym, 2);
    r.step();
    REQUIRE(requests(*r.recap).size() == 4);
    r.step(100);
    rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].generation == 2);
    CHECK(rs[0].attempt == 1);
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

TEST_CASE("recap manager: RecapReject and a non-Ok Status both report REJECTED and retry") {
    Rig r;
    const uint32_t sym = r.symbols.intern_u64(7);
    r.request(sym);
    r.step();
    r.recap->push(s_reject(requests(*r.recap)[0].id, sp_proto::RejectReason::Busy));
    r.step();
    auto rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].status == RecapStatus::REJECTED);
    CHECK(rs[0].reason == 3);
    CHECK(r.c().recap_rejects == 1);
    r.step(40);
    REQUIRE(requests(*r.recap).size() == 2);

    r.recap->push(s_status(requests(*r.recap)[1].id, sp_proto::StatusCode::Unsupported));
    r.step();
    rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].status == RecapStatus::REJECTED);
    CHECK(rs[0].reason == 0x100 + 1);
    CHECK(r.c().recap_rejects == 2);
}

TEST_CASE("recap manager: stray records, unknown requests and short replies are counted, not delivered") {
    Rig r;
    const uint32_t sym = r.symbols.intern_u64(7);
    r.request(sym);
    r.step();
    const uint64_t id = requests(*r.recap)[0].id;
    r.recap->push(s_record(id, trade_msg(7, 1)));                 // outside any reply
    r.recap->push(s_reply(99, 1, {trade_msg(7, 2)}));             // nobody asked
    r.recap->push(cat({s_begin(id, 20), s_record(id, trade_msg(7, 3)), s_end(id, 2)})); // claims 2, carried 1
    r.step();
    CHECK(r.replies().empty());
    CHECK(r.c().recap_stray == 3); // the lone record, the unknown begin, the short reply
    r.recap->push(s_reply(id, 20, {trade_msg(7, 3)}));            // the service sends it again, whole
    r.step();
    auto rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].request_id == id);
    CHECK(rs[0].messages == 1);
}

TEST_CASE("recap manager: SPP framing and unknown messages") {
    Rig r;
    const uint32_t sym = r.symbols.intern_u64(7);
    r.request(sym);
    r.step();
    const size_t before = sent_messages(*r.recap).size();
    // A length running past the frame ends the frame.
    std::vector<uint8_t> bad = {0xFF, 0xFF, 0, 0, 1, 2};
    r.recap->push(bad);
    // An unknown core template with a correlation id is answered Unsupported.
    std::vector<uint8_t> unk;
    put(unk, 16, 4);
    put(unk, 8, 2); put(unk, 555, 2); put(unk, sp_proto::kSchemaId, 2); put(unk, 1, 2);
    put(unk, 0x77, 8);
    // A message from another schema is stray, never answered.
    std::vector<uint8_t> other;
    put(other, 16, 4);
    put(other, 8, 2); put(other, 10, 2); put(other, 7, 2); put(other, 1, 2);
    put(other, 0x78, 8);
    // A must-understand extension is answered Unsupported; a plain one is ignored.
    auto ext_mu = spp<sp_proto::ExtensionEncoder>([](auto& e) { e.CorrelationId(0x79).ExtensionId(0x8001).Flags(1); });
    auto ext = spp<sp_proto::ExtensionEncoder>([](auto& e) { e.CorrelationId(0x7A).ExtensionId(0x8001); });
    r.recap->push(cat({unk, other, ext_mu, ext}));
    r.step();
    CHECK(r.c().recap_stray == 3); // the bad frame, the unknown template, the other schema
    const auto msgs = sent_messages(*r.recap);
    REQUIRE(msgs.size() == before + 2);
    std::vector<uint64_t> answered;
    for (size_t i = before; i < msgs.size(); ++i) {
        REQUIRE(msgs[i].template_id == sp_proto::StatusDecoder::kTemplateId);
        sp_proto::StatusDecoder d;
        REQUIRE(d.wrap(msgs[i].msg.data(), msgs[i].msg.size()));
        CHECK(d.Code() == sp_proto::StatusCode::Unsupported);
        answered.push_back(d.CorrelationId());
    }
    CHECK(answered == std::vector<uint64_t>{0x77, 0x79});

    // A newer service appends a field to RecapBegin: read what is known.
    const uint64_t id = requests(*r.recap)[0].id;
    std::vector<uint8_t> nb;
    put(nb, 8 + 40, 4);
    put(nb, 40, 2); put(nb, 11, 2); put(nb, sp_proto::kSchemaId, 2); put(nb, 2, 2);
    put(nb, id, 8); put(nb, 321, 8); put(nb, 0, 8); put(nb, 0, 4); put(nb, 0, 4); put(nb, 0xEEEE, 8);
    r.recap->push(cat({nb, s_end(id, 0)}));
    r.step();
    auto rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].status == RecapStatus::OK);
    CHECK(rs[0].as_of == 321);
}

TEST_CASE("recap manager: Hello again after a reconnect; an incompatible service gets nothing but Status") {
    Rig r;
    r.step();
    REQUIRE(sent_messages(*r.recap).size() == 1); // Hello on connect, before any request
    r.recap->push(s_hello(1, 3, "binance-snap"));
    r.step();
    CHECK(sent_messages(*r.recap).size() == 1);   // a compatible Hello needs no answer

    r.recap->link_up = false;
    r.step();
    r.recap->link_up = true;
    r.step();
    auto msgs = sent_messages(*r.recap);
    REQUIRE(msgs.size() == 2);
    CHECK(msgs[1].template_id == sp_proto::HelloDecoder::kTemplateId);

    // The new service only speaks 2..3.
    r.recap->push(s_hello(2, 3, "future-snap"));
    r.step();
    msgs = sent_messages(*r.recap);
    REQUIRE(msgs.size() == 3);
    CHECK(msgs[2].template_id == sp_proto::StatusDecoder::kTemplateId);
    const uint32_t sym = r.symbols.intern_u64(7);
    r.request(sym);
    r.step();
    CHECK(requests(*r.recap).empty());
    auto rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].status == RecapStatus::SEND_FAILED);

    // A reconnect forgets it: the next service may be compatible.
    r.recap->link_up = false;
    r.step();
    r.recap->link_up = true;
    r.step(40);
    CHECK(requests(*r.recap).size() == 1);
}

TEST_CASE("recap manager: a reply larger than the slab is an overflow; slabs are reused after release") {
    Rig r;
    const uint32_t a = r.symbols.intern_u64(1), b = r.symbols.intern_u64(2), c = r.symbols.intern_u64(3);
    r.request(a);
    r.step();
    const uint64_t ida = requests(*r.recap)[0].id;
    std::vector<uint8_t> big = s_begin(ida, 1);
    for (int i = 0; i < 100; ++i) { auto t = s_record(ida, trade_msg(1, static_cast<uint64_t>(i))); big.insert(big.end(), t.begin(), t.end()); } // 100 * 51 > 4096
    auto e = s_end(ida, 100);
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
    REQUIRE(requests(*r.recap).size() == 4);
    auto id_of = [&](uint64_t instrument) {
        const auto rq = requests(*r.recap);
        for (auto it = rq.rbegin(); it != rq.rend(); ++it) if (it->instrument == instrument) return it->id;
        return uint64_t{0};
    };
    r.recap->push(s_reply(id_of(2), 1, {}));
    r.recap->push(s_reply(id_of(3), 1, {}));
    r.recap->push(s_reply(id_of(1), 1, {}));
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
    REQUIRE(requests(*r.recap).size() == 5);
    r.recap->push(s_reply(requests(*r.recap).back().id, 1, {}));
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
    REQUIRE(requests(*r.recap).size() == 1);
    CHECK(requests(*r.recap)[0].instrument == 55);
    CHECK(requests(*r.recap)[0].venue == 10);
}

TEST_CASE("recap manager, reply: stream_markers: markers in the stream's schema, across fragments; stray and reject") {
    Rig r(partition_yaml("", 4, kMarkers));
    const uint32_t sym = r.symbols.intern_u64(4242);
    r.request(sym);
    r.step();
    REQUIRE(requests(*r.recap).size() == 1); // the request is SPP in either mode
    const uint64_t id = requests(*r.recap)[0].id;
    const auto t1 = trade_msg(4242, 1), t2 = trade_msg(4242, 2);
    r.recap->push(trade_msg(4242, 9));                     // outside any reply
    r.recap->push(cat({begin_msg(99, 1), t1, end_msg(99)})); // nobody asked
    r.recap->push(cat({begin_msg(id, 5000), t1}));
    r.recap->push(t2);
    r.recap->push(end_msg(id));
    r.step();
    auto rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].status == RecapStatus::OK);
    CHECK(rs[0].as_of == 5000);
    REQUIRE(rs[0].messages == 2);
    auto recs = records(rs[0]);
    CHECK(recs[0] == t1);
    CHECK(recs[1] == t2);
    CHECK(r.c().recap_stray == 2);

    r.request(sym);
    r.step();
    r.recap->push(reject_msg(requests(*r.recap)[1].id, 3));
    r.step();
    rs = r.replies();
    REQUIRE(rs.size() == 1);
    CHECK(rs[0].status == RecapStatus::REJECTED);
    CHECK(rs[0].reason == 3);
}

TEST_CASE("recap config: what a partition needs before it can recap") {
    auto build = [](const std::string& yaml, std::string& err) {
        PartitionConfig cfg;
        if (!PartitionConfig::parse(YAML::Load(yaml), 0, data_dir(), cfg, err)) return false;
        Partition p;
        return p.build(cfg, err, std::make_unique<FakeTransport>());
    };
    std::string err;
    // Markers missing: fine for spp, refused for stream_markers.
    std::string y = partition_yaml();
    y.erase(y.find("    - { template: 100, admin: RECAP_END"), std::string("    - { template: 100, admin: RECAP_END,    fields: { request_id: SeqNum } }\n").size());
    const bool built = build(y, err); // with Aeron built in, it fails later, on the aeron keys
    CHECK_MESSAGE((built || err.find("RECAP_BEGIN") == std::string::npos), err);
    CHECK_FALSE(build(y + kMarkers, err));
    CHECK_MESSAGE(err.find("needs the decoder to map RECAP_BEGIN and RECAP_END") != std::string::npos, err);
    // Unknown reply mode.
    CHECK_FALSE(build(partition_yaml() + "  reply: carrier_pigeon\n", err));
    CHECK_MESSAGE(err.find("recap.reply must be spp or stream_markers") != std::string::npos, err);
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
    y = partition_yaml();
    y.replace(y.find("coalesce_ms: 0"), 14, "coalesce_ms: 10001");
    CHECK_FALSE(build(y, err));
    CHECK_MESSAGE(err.find("recap.coalesce_ms must be 0..10000") != std::string::npos, err);
    // dbn recaps over spp only.
    CHECK_FALSE(build(R"(
name: d
transport: { type: udp, group: "", port: 1 }
decoder: { type: dbn }
recap: { type: aeron, reply: stream_markers }
)", err));
    CHECK_MESSAGE(err.find("dbn cannot use reply: stream_markers") != std::string::npos, err);
    {
        PartitionConfig cfg;
        REQUIRE_MESSAGE(PartitionConfig::parse(YAML::Load(R"(
name: d
transport: { type: udp, group: "", port: 1 }
framing: { length: packet, sequence: message }
decoder: { type: dbn }
recap: { type: aeron }
)"), 0, data_dir(), cfg, err), err);
        Partition p;
        CHECK_MESSAGE(p.build(cfg, err, std::make_unique<FakeTransport>(), std::make_unique<FakeRecap>()), err);
        CHECK(p.has_recap());
    }
#if SUBPLANE_WITH_AERON
    // aeron component keys are checked when the component is built.
    CHECK_FALSE(build(partition_yaml(), err));
    CHECK_MESSAGE(err.find("recap.request must be") != std::string::npos, err);
#endif
}
