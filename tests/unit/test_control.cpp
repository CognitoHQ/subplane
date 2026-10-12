#include <doctest.h>

#include "fake_transport.h"
#include "flashtick/client/control_client.h"
#include "flashtick/client/shm_reader.h"
#include "flashtick/control/control_server.h"
#include "flashtick/core/clock.h"
#include "flashtick/pipeline/session_manager.h"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <mutex>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace flashtick;
using flashtick::test::FakeTransport;

namespace {

std::string data_dir() {
    std::string f = __FILE__;
    return f.substr(0, f.find_last_of('/')) + "/../data";
}

bool exists(const std::string& p) { struct stat st {}; return ::stat(p.c_str(), &st) == 0; }

std::string tmp_dir() {
    const char* t = std::getenv("TMPDIR");
    return (t && *t) ? std::string(t) : std::string("/tmp");
}

void sleep_ms(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

// FakeTransport is single-threaded; here the pipeline thread polls while the
// test thread pushes, so serialize the two with a mutex (test-only).
class LockedTransport final : public Transport {
public:
    bool open(std::string& err) override { std::lock_guard<std::mutex> l(m_); return inner_.open(err); }
    int poll(PacketBatch& out) noexcept override { std::lock_guard<std::mutex> l(m_); return inner_.poll(out); }
    void close() noexcept override { std::lock_guard<std::mutex> l(m_); inner_.close(); }
    std::string describe() const override { return "locked-fake"; }
    int fd() const noexcept override { return -1; }
    void push(std::vector<uint8_t> b, uint64_t ts) { std::lock_guard<std::mutex> l(m_); inner_.push(std::move(b), ts); }
private:
    std::mutex m_;
    FakeTransport inner_;
};

void put_u16le(std::vector<uint8_t>& b, uint16_t v) { b.push_back(static_cast<uint8_t>(v)); b.push_back(static_cast<uint8_t>(v >> 8)); }

std::vector<uint8_t> sbe_msg(const SbeSchema& s, uint16_t tid, uint64_t sec, uint64_t seq) {
    const MessageDef* m = s.find_message(tid);
    REQUIRE(m);
    std::vector<uint8_t> b;
    put_u16le(b, m->block_length); put_u16le(b, tid); put_u16le(b, s.schema_id()); put_u16le(b, s.version());
    std::vector<uint8_t> root(m->block_length, 0);
    auto set = [&](const char* n, uint64_t v) {
        const FieldDef* f = s.find_field(*m, n);
        REQUIRE_MESSAGE(f, n);
        for (uint16_t i = 0; i < f->size && i < 8; ++i) root[f->offset + i] = static_cast<uint8_t>(v >> (8 * i));
    };
    set("SecurityID", sec); set("SeqNum", seq); set("TransactTime", 1);
    b.insert(b.end(), root.begin(), root.end());
    return b;
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
heartbeat_timeout_ms: 60000
)";

constexpr uint64_t kLingerNs = 200'000'000ull;
std::atomic<int> g_seq{0};

struct CH {
    SymbolTable symbols{1024};
    Lvc lvc;
    Partition part;
    LockedTransport* fake = nullptr;
    Pipeline pipe;
    std::unique_ptr<SessionManager> sm;
    std::unique_ptr<ControlServer> srv;
    std::string agent;
    std::string ring_dir;
    std::string sock_path;

    explicit CH(std::string primary_sock = "") : pipe("g0", 99, symbols, lvc) {
        std::string err;
        REQUIRE(lvc.init(1024, {static_cast<uint8_t>(MsgType::TRADE), static_cast<uint8_t>(MsgType::QUOTE)}, 256, err));
        PartitionConfig cfg;
        REQUIRE_MESSAGE(PartitionConfig::parse(YAML::Load(kPartitionYaml), 0, data_dir(), cfg, err), err);
        auto ft = std::make_unique<LockedTransport>();
        fake = ft.get();
        REQUIRE_MESSAGE(part.build(cfg, err, std::move(ft)), err);
        REQUIRE(part.open(err));
        pipe.add_partition(&part);

        const int n = g_seq.fetch_add(1);
        agent = "ctltest" + std::to_string(getpid()) + "_" + std::to_string(n);
        ring_dir = ::access("/dev/shm", W_OK) == 0 ? "/dev/shm" : tmp_dir();
        sock_path = tmp_dir() + "/ft_ctl_" + std::to_string(getpid()) + "_" + std::to_string(n) + ".sock";

        SessionManagerConfig sc;
        sc.ring_dir = ring_dir;
        sc.agent_name = agent;
        sc.epoch = 99;
        sc.max_symbols = 1024;
        sc.linger_ns = kLingerNs;
        sm = std::make_unique<SessionManager>(sc, symbols, lvc, std::vector<Partition*>{&part}, std::vector<Pipeline*>{&pipe});
        REQUIRE_MESSAGE(pipe.start(-1, err), err);

        ControlServer::Config cc;
        cc.socket_path = primary_sock.empty() ? sock_path : primary_sock;
        cc.fallback_socket_path = sock_path;
        cc.linger_ns = kLingerNs;
        srv = std::make_unique<ControlServer>(cc, *sm, symbols, std::vector<Partition*>{&part}, agent, 99);
        REQUIRE_MESSAGE(srv->start(err), err);
        CHECK(srv->bound_path() == sock_path);
    }

    ~CH() {
        srv->stop();
        sm->destroy_all();
        pipe.stop();
        CHECK_FALSE(exists(sock_path));
        // No ring left behind for this agent.
        const std::string prefix = "flashtick." + agent + ".";
        if (DIR* d = ::opendir(ring_dir.c_str())) {
            while (dirent* e = ::readdir(d)) {
                CHECK_MESSAGE(std::strncmp(e->d_name, prefix.c_str(), prefix.size()) != 0, e->d_name);
            }
            ::closedir(d);
        }
    }

    bool has_session(const std::string& name) const {
        for (const auto& s : sm->status()) if (s.name == name) return true;
        return false;
    }
    uint64_t filtered(const std::string& name) const {
        for (const auto& s : sm->status()) if (s.name == name) return s.filtered;
        return 0;
    }
};

// Polls until a record arrives or the timeout expires.
bool next_rec(ShmReader& rd, TickHeader& hdr, std::vector<uint8_t>& payload, uint64_t timeout_ms = 2000) {
    const uint64_t deadline = now_mono_ns() + timeout_ms * 1'000'000ull;
    RecordView v;
    while (now_mono_ns() < deadline) {
        const ReaderStatus st = rd.poll(v);
        if (st == ReaderStatus::Ok) {
            hdr = *v.hdr;
            payload.assign(v.payload, v.payload + v.payload_len);
            REQUIRE(v.valid());
            return true;
        }
        if (st != ReaderStatus::Empty) return false;
        std::this_thread::yield();
    }
    return false;
}

template <class Pred>
bool wait_for(Pred p, uint64_t timeout_ms = 3000) {
    const uint64_t deadline = now_mono_ns() + timeout_ms * 1'000'000ull;
    while (now_mono_ns() < deadline) {
        if (p()) return true;
        sleep_ms(2);
    }
    return p();
}

constexpr uint8_t code(ControlCode c) { return static_cast<uint8_t>(c); }
constexpr uint64_t bit(MsgType t) { return 1ull << static_cast<uint8_t>(t); }

} // namespace

TEST_CASE("control: hello, dynamic session lifecycle, snapshot, symbols, linger, attach, ping") {
    CH h;
    ControlClient cli;
    REQUIRE_MESSAGE(cli.connect(h.sock_path), cli.last_error());
    CHECK(wait_for([&] { return h.srv->clients() == 1; }));

    // HELLO
    HelloInfo hi;
    REQUIRE_MESSAGE(cli.hello(hi), cli.last_error());
    CHECK(hi.agent_name == h.agent);
    CHECK(hi.epoch == 99);
    CHECK(hi.record_version == RECORD_VERSION);
    CHECK(hi.proto_version == CONTROL_PROTO_VERSION);
    CHECK(hi.max_symbols == 1024);
    CHECK(hi.schema_id == h.part.schema().schema_id());
    REQUIRE(hi.partitions.size() == 1);
    CHECK(hi.partitions[0].id == 0);
    CHECK(hi.partitions[0].name == "p0");

    // CREATE_SESSION: TRADE only, all symbols
    SessionRequest req;
    req.name = "dyn";
    req.ring_capacity_bytes = 1u << 20;
    req.max_payload = 1024;
    req.msg_type_mask = bit(MsgType::TRADE);
    AttachInfo ai;
    REQUIRE_MESSAGE(cli.create_session(req, ai), cli.last_error());
    CHECK(ai.name == "dyn");
    CHECK(ai.owner == "client");
    CHECK(ai.epoch == 99);
    CHECK(ai.capacity == (1u << 20));
    CHECK(ai.max_payload == 1024);
    CHECK(ai.filter_generation == 1);
    ShmReader rd;
    std::string err;
    REQUIRE_MESSAGE(rd.open(ai.ring_path, &err), err);
    CHECK(rd.epoch() == 99);

    TickHeader th;
    std::vector<uint8_t> pl;
    REQUIRE(next_rec(rd, th, pl));
    CHECK(th.msg_type == code(ControlCode::AGENT_RESTARTED));

    // Trade + quote through the live pipeline thread; only the trade passes.
    const auto t1 = sbe_msg(h.part.schema(), 1, 1001, 1);
    const auto q1 = sbe_msg(h.part.schema(), 2, 1001, 2);
    std::vector<uint8_t> pkt = t1;
    pkt.insert(pkt.end(), q1.begin(), q1.end());
    h.fake->push(pkt, now_ns());
    REQUIRE(wait_for([&] { return h.filtered("dyn") >= 1; }));
    REQUIRE(next_rec(rd, th, pl));
    CHECK(th.msg_type == code(ControlCode::SYMBOL_MAPPED));
    REQUIRE(next_rec(rd, th, pl));
    CHECK(th.kind == static_cast<uint8_t>(Kind::DATA));
    CHECK(th.msg_type == static_cast<uint8_t>(MsgType::TRADE));
    CHECK(th.sequence == 1);
    CHECK(pl == t1);
    const uint32_t sym_id = th.symbol_id;
    CHECK_FALSE(next_rec(rd, th, pl, 50)); // the quote was filtered

    // SET_FILTER -> QUOTE
    req.msg_type_mask = bit(MsgType::QUOTE);
    uint32_t gen = 0;
    REQUIRE_MESSAGE(cli.set_filter(req, gen), cli.last_error());
    CHECK(gen == 2);
    REQUIRE(next_rec(rd, th, pl));
    CHECK(th.msg_type == code(ControlCode::FILTER_RELOADED));
    FilterReloadedEvent fr {};
    REQUIRE(pl.size() == sizeof(fr));
    std::memcpy(&fr, pl.data(), sizeof(fr));
    CHECK(fr.generation == 2);
    CHECK(fr.session_id == ai.id);

    // SNAPSHOT: the LVC quote (filter now QUOTE), not the trade
    uint64_t ring_head = ~0ull;
    std::vector<std::pair<TickHeader, std::vector<uint8_t>>> snap;
    REQUIRE_MESSAGE(cli.snapshot("dyn", ring_head, [&](const TickHeader& sh, const uint8_t* p, uint32_t n) {
        snap.emplace_back(sh, std::vector<uint8_t>(p, p + n));
    }), cli.last_error());
    CHECK(ring_head == rd.head());
    CHECK(ring_head > 0);
    REQUIRE(snap.size() == 1);
    CHECK(snap[0].first.msg_type == static_cast<uint8_t>(MsgType::QUOTE));
    CHECK(snap[0].first.sequence == 2);
    CHECK(snap[0].first.symbol_id == sym_id);
    CHECK((snap[0].first.flags & FLAG_SNAPSHOT_ORIGIN) != 0);
    CHECK(snap[0].second == q1);

    // SNAPSHOT of an unknown session
    CHECK_FALSE(cli.snapshot("nope", ring_head, nullptr));
    CHECK(cli.last_status() == CtlStatus::NOT_FOUND);

    // SYMBOLS
    std::vector<std::pair<uint32_t, std::string>> syms;
    REQUIRE_MESSAGE(cli.symbols(syms), cli.last_error());
    CHECK(std::find(syms.begin(), syms.end(), std::make_pair(sym_id, std::string("1001"))) != syms.end());

    // Name collision
    AttachInfo dup;
    CHECK_FALSE(cli.create_session(req, dup));
    CHECK(cli.last_status() == CtlStatus::EXISTS);

    // Bad requests are reported, not fatal
    SessionRequest bad = req;
    bad.name = "bad/name";
    CHECK_FALSE(cli.create_session(bad, dup));
    CHECK(cli.last_status() == CtlStatus::BAD_REQUEST);
    bad = req;
    bad.name = "badpart";
    bad.partition_mask = 1ull << 5;
    CHECK_FALSE(cli.create_session(bad, dup));
    CHECK(cli.last_status() == CtlStatus::BAD_REQUEST);
    bad.partition_mask = 0;
    bad.ring_capacity_bytes = 1000;
    CHECK_FALSE(cli.create_session(bad, dup));
    CHECK(cli.last_status() == CtlStatus::BAD_REQUEST);
    CHECK_FALSE(h.has_session("badpart"));

    // DESTROY_SESSION
    REQUIRE_MESSAGE(cli.destroy_session("dyn"), cli.last_error());
    bool closing = false;
    while (next_rec(rd, th, pl, 500)) {
        if (th.msg_type == code(ControlCode::SESSION_CLOSING)) { closing = true; break; }
    }
    CHECK(closing);
    CHECK_FALSE(exists(ai.ring_path));
    CHECK_FALSE(h.has_session("dyn"));
    CHECK_FALSE(cli.destroy_session("dyn"));
    CHECK(cli.last_status() == CtlStatus::NOT_FOUND);

    // Linger: a client-owned session outlives its socket by linger_ns, then goes.
    {
        ControlClient c2;
        REQUIRE(c2.connect(h.sock_path));
        SessionRequest lr;
        lr.name = "linger";
        lr.ring_capacity_bytes = 1u << 16;
        lr.max_payload = 1024;
        AttachInfo li;
        REQUIRE_MESSAGE(c2.create_session(lr, li), c2.last_error());
        c2.close();
        const uint64_t closed_at = now_mono_ns();
        REQUIRE(wait_for([&] { return h.srv->clients() == 1; }));
        if (now_mono_ns() - closed_at < kLingerNs / 2) CHECK(h.has_session("linger"));
        REQUIRE(wait_for([&] { return !h.has_session("linger"); }, 3000));
        CHECK(now_mono_ns() - closed_at >= kLingerNs);
        CHECK_FALSE(exists(li.ring_path));
    }

    // Re-ATTACH within linger keeps a client-owned session alive.
    {
        ControlClient c3;
        REQUIRE(c3.connect(h.sock_path));
        SessionRequest rr;
        rr.name = "reattach";
        rr.ring_capacity_bytes = 1u << 16;
        rr.max_payload = 1024;
        AttachInfo ri;
        REQUIRE(c3.create_session(rr, ri));
        c3.close();
        REQUIRE(wait_for([&] { return h.srv->clients() == 1; }));
        AttachInfo ri2;
        REQUIRE_MESSAGE(cli.attach("reattach", ri2), cli.last_error());
        CHECK(ri2.ring_path == ri.ring_path);
        sleep_ms(static_cast<int>(2 * kLingerNs / 1'000'000) + 150);
        CHECK(h.has_session("reattach"));
        REQUIRE(cli.destroy_session("reattach"));
    }

    // Config-owned session: attachable, survives client disconnect, not client-mutable.
    {
        SessionSpec cs;
        cs.name = "cfg";
        cs.owner = "config";
        cs.ring_capacity_bytes = 1u << 16;
        cs.max_payload = 1024;
        cs.all_symbols = true;
        REQUIRE_MESSAGE(h.sm->create(cs, err), err);
        ControlClient c4;
        REQUIRE(c4.connect(h.sock_path));
        AttachInfo ci;
        REQUIRE_MESSAGE(c4.attach("cfg", ci), c4.last_error());
        CHECK(ci.owner == "config");
        CHECK(ci.epoch == 99);
        ShmReader crd;
        CHECK(crd.open(ci.ring_path));
        SessionRequest cr;
        cr.name = "cfg";
        CHECK_FALSE(c4.set_filter(cr, gen));
        CHECK(c4.last_status() == CtlStatus::UNSUPPORTED);
        CHECK_FALSE(c4.destroy_session("cfg"));
        CHECK(c4.last_status() == CtlStatus::UNSUPPORTED);
        c4.close();
        REQUIRE(wait_for([&] { return h.srv->clients() == 1; }));
        sleep_ms(static_cast<int>(kLingerNs / 1'000'000) + 250);
        CHECK(h.has_session("cfg"));
    }

    CHECK_FALSE(cli.attach("missing", ai));
    CHECK(cli.last_status() == CtlStatus::NOT_FOUND);

    uint64_t rtt = 0;
    REQUIRE_MESSAGE(cli.ping(rtt), cli.last_error());
    CHECK(rtt > 0);
    CHECK(rtt < 1'000'000'000ull);
}

TEST_CASE("control: bad version, short and oversized datagrams do not kill the server; fallback path") {
    CH h("/nonexistent-flashtick-dir/ctl.sock"); // ENOENT -> fallback (asserted in CH)
    ControlClient cli;
    REQUIRE(cli.connect(h.sock_path));

    // Wrong protocol version
    CtlHeader bad {};
    bad.op = static_cast<uint16_t>(CtlOp::HELLO);
    bad.version = CONTROL_PROTO_VERSION + 7;
    bad.req_id = 4242;
    REQUIRE(cli.send_raw(&bad, sizeof(bad)));
    CtlHeader rh {};
    std::vector<uint8_t> body;
    CHECK_FALSE(cli.recv_reply(CtlOp::HELLO, 4242, rh, body));
    CHECK(rh.status == static_cast<uint32_t>(CtlStatus::BAD_REQUEST));
    CHECK(cli.last_status() == CtlStatus::BAD_REQUEST);

    // len field disagrees with the datagram
    CtlHeader lie {};
    lie.op = static_cast<uint16_t>(CtlOp::PING);
    lie.version = CONTROL_PROTO_VERSION;
    lie.req_id = 4243;
    lie.len = 100;
    REQUIRE(cli.send_raw(&lie, sizeof(lie)));
    CHECK_FALSE(cli.recv_reply(CtlOp::PING, 4243, rh, body));
    CHECK(cli.last_status() == CtlStatus::BAD_REQUEST);

    // Shorter than a header
    const uint8_t tiny[5] = {1, 2, 3, 4, 5};
    REQUIRE(cli.send_raw(tiny, sizeof(tiny)));
    CHECK_FALSE(cli.recv_reply(static_cast<CtlOp>(0), 0, rh, body));
    CHECK(cli.last_status() == CtlStatus::BAD_REQUEST);

    // Unknown op
    CtlHeader unk {};
    unk.op = 999;
    unk.version = CONTROL_PROTO_VERSION;
    unk.req_id = 4244;
    REQUIRE(cli.send_raw(&unk, sizeof(unk)));
    CHECK_FALSE(cli.recv_reply(static_cast<CtlOp>(999), 4244, rh, body));
    CHECK(cli.last_status() == CtlStatus::UNSUPPORTED);

    // Oversized datagram (100 KiB > 64 KiB limit)
    std::vector<uint8_t> big(100 * 1024, 0xAB);
    CtlHeader bh {};
    bh.op = static_cast<uint16_t>(CtlOp::CREATE_SESSION);
    bh.version = CONTROL_PROTO_VERSION;
    bh.req_id = 4245;
    bh.len = static_cast<uint32_t>(big.size() - sizeof(bh));
    std::memcpy(big.data(), &bh, sizeof(bh));
    REQUIRE_MESSAGE(cli.send_raw(big.data(), big.size()), cli.last_error());
    CHECK_FALSE(cli.recv_reply(CtlOp::CREATE_SESSION, 4245, rh, body));
    CHECK(cli.last_status() == CtlStatus::TOO_LARGE);

    // Server and connection still healthy
    uint64_t rtt = 0;
    REQUIRE_MESSAGE(cli.ping(rtt), cli.last_error());
    HelloInfo hi;
    REQUIRE(cli.hello(hi));
    CHECK(hi.epoch == 99);
    CHECK(h.srv->clients() == 1);

    // A second, well-behaved client is unaffected
    ControlClient c2;
    REQUIRE(c2.connect(h.sock_path));
    REQUIRE(c2.ping(rtt));

    // Stale socket from a dead process is replaced; a live one is refused.
    ControlServer::Config cc;
    cc.socket_path = h.sock_path;
    ControlServer dup(cc, *h.sm, h.symbols, std::vector<Partition*>{&h.part}, h.agent, 99);
    std::string err;
    CHECK_FALSE(dup.start(err));
    CHECK(err.find("in use") != std::string::npos);
}
