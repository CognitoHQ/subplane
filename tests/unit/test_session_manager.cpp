#include <doctest.h>

#include "fake_transport.h"
#include "flashtick/client/shm_reader.h"
#include "flashtick/core/clock.h"
#include "flashtick/pipeline/session_manager.h"

#include <cstring>
#include <sys/stat.h>
#include <unistd.h>

using namespace flashtick;
using flashtick::test::FakeTransport;

namespace {

std::string data_dir() {
    std::string f = __FILE__;
    return f.substr(0, f.find_last_of('/')) + "/../data";
}

const char* kPartitionYaml = R"(
name: p0
transport: { type: udp_multicast, group: "", port: 1 }
framing: { sequence: message.SeqNum }
decoder:
  type: sbe_routing
  schema: sample_md.xml
  fields: { symbol: SecurityID, sequence: SeqNum, exchange_ts: TransactTime, venue: MarketSegmentID }
  messages:
    - { template: 1, msg_type: TRADE }
    - { template: 2, msg_type: QUOTE }
)";

void put_u16le(std::vector<uint8_t>& b, uint16_t v) { b.push_back(static_cast<uint8_t>(v)); b.push_back(static_cast<uint8_t>(v >> 8)); }

std::vector<uint8_t> trade(const SbeSchema& s, uint64_t sec, uint64_t seq) {
    const MessageDef* m = s.find_message(1);
    std::vector<uint8_t> b;
    put_u16le(b, m->block_length); put_u16le(b, 1); put_u16le(b, s.schema_id()); put_u16le(b, s.version());
    std::vector<uint8_t> root(m->block_length, 0);
    auto set = [&](const char* n, uint64_t v) { const FieldDef* f = s.find_field(*m, n); for (uint16_t i = 0; i < f->size && i < 8; ++i) root[f->offset + i] = static_cast<uint8_t>(v >> (8 * i)); };
    set("SecurityID", sec); set("SeqNum", seq); set("TransactTime", 1);
    b.insert(b.end(), root.begin(), root.end());
    return b;
}

bool exists(const std::string& p) { struct stat st{}; return ::stat(p.c_str(), &st) == 0; }

struct H {
    SymbolTable symbols{256};
    Lvc lvc;
    Partition part;
    FakeTransport* fake = nullptr;
    Pipeline pipe;
    std::unique_ptr<SessionManager> sm;
    std::string agent_name;

    H() : pipe("g0", 77, symbols, lvc) {
        std::string err;
        REQUIRE(lvc.init(256, {static_cast<uint8_t>(MsgType::TRADE)}, 256, err));
        PartitionConfig cfg;
        REQUIRE_MESSAGE(PartitionConfig::parse(YAML::Load(kPartitionYaml), 0, data_dir(), cfg, err), err);
        auto ft = std::make_unique<FakeTransport>();
        fake = ft.get();
        REQUIRE(part.build(cfg, err, std::move(ft)));
        REQUIRE(part.open(err));
        pipe.add_partition(&part);
        agent_name = "smtest" + std::to_string(getpid());
        SessionManagerConfig c;
        c.agent_name = agent_name;
        c.epoch = 77;
        c.max_symbols = 256;
        c.linger_ns = 50'000'000;
        sm = std::make_unique<SessionManager>(c, symbols, lvc, std::vector<Partition*>{&part}, std::vector<Pipeline*>{&pipe});
    }
    SessionSpec spec(const char* name, std::vector<std::string> types, std::string owner = "config") {
        SessionSpec s;
        s.name = name;
        s.owner = owner;
        s.ring_capacity_bytes = 1u << 20;
        s.max_payload = 1024;
        s.all_symbols = true;
        s.msg_types = std::move(types);
        return s;
    }
};

} // namespace

TEST_CASE("SessionManager: create, attach info, data delivery, set_filter, destroy") {
    H h;
    std::string err;
    REQUIRE_MESSAGE(h.sm->create(h.spec("a", {"TRADE"}), err), err);
    CHECK_FALSE(h.sm->create(h.spec("a", {"TRADE"}), err)); // collision
    SessionAttachInfo info;
    REQUIRE(h.sm->attach_info("a", info));
    CHECK(info.epoch == 77);
    CHECK(info.filter_generation == 1);
    CHECK(exists(info.ring_path));
    ShmReader rd;
    REQUIRE(rd.open(info.ring_path));
    CHECK(rd.epoch() == 77);
    RecordView v;
    REQUIRE(rd.poll(v) == ReaderStatus::Ok); // AGENT_RESTARTED
    CHECK(v.hdr->msg_type == static_cast<uint8_t>(ControlCode::AGENT_RESTARTED));

    h.fake->push(trade(h.part.schema(), 42, 1), now_ns());
    h.pipe.run_once();
    REQUIRE(rd.poll(v) == ReaderStatus::Ok); // SYMBOL_MAPPED
    REQUIRE(rd.poll(v) == ReaderStatus::Ok);
    CHECK(v.hdr->msg_type == static_cast<uint8_t>(MsgType::TRADE));

    REQUIRE_MESSAGE(h.sm->set_filter(h.spec("a", {"QUOTE"}), err), err);
    REQUIRE(h.sm->attach_info("a", info));
    CHECK(info.filter_generation == 2);
    REQUIRE(rd.poll(v) == ReaderStatus::Ok);
    CHECK(v.hdr->msg_type == static_cast<uint8_t>(ControlCode::FILTER_RELOADED));
    h.fake->push(trade(h.part.schema(), 42, 2), now_ns());
    h.pipe.run_once();
    CHECK(rd.poll(v) == ReaderStatus::Empty); // trade filtered out now

    // snapshot sees the latest trade only if the filter accepts TRADE; switch back
    REQUIRE(h.sm->set_filter(h.spec("a", {"TRADE"}), err));
    uint64_t head = 0;
    int n = 0;
    uint64_t snap_seq = 0;
    uint8_t snap_flags = 0;
    const bool ok = h.sm->snapshot("a", head, [&](const TickHeader& th, const uint8_t*, uint32_t) {
        snap_seq = th.sequence;
        snap_flags = th.flags;
        ++n;
    });
    REQUIRE(ok);
    CHECK(n == 1);
    CHECK(snap_seq == 2);
    CHECK((snap_flags & FLAG_SNAPSHOT_ORIGIN) != 0);
    CHECK(head == rd.head());

    REQUIRE(h.sm->destroy("a", 1, err));
    REQUIRE(rd.poll(v) == ReaderStatus::Ok);
    while (v.hdr->msg_type != static_cast<uint8_t>(ControlCode::SESSION_CLOSING)) REQUIRE(rd.poll(v) == ReaderStatus::Ok);
    CHECK_FALSE(exists(info.ring_path));
    CHECK(h.sm->count() == 0);
}

TEST_CASE("SessionManager: reconcile_config creates, updates and removes config-owned sessions only") {
    H h;
    std::string err;
    REQUIRE(h.sm->create(h.spec("dyn", {"TRADE"}, "client"), err));
    std::vector<SessionSpec> cfg = {h.spec("x", {"TRADE"}), h.spec("y", {"QUOTE"})};
    REQUIRE_MESSAGE(h.sm->reconcile_config(cfg, err), err);
    CHECK(h.sm->count() == 3);
    cfg = {h.spec("x", {"QUOTE"})}; // y gone, x changed
    REQUIRE_MESSAGE(h.sm->reconcile_config(cfg, err), err);
    CHECK(h.sm->count() == 2);
    SessionAttachInfo info;
    REQUIRE(h.sm->attach_info("x", info));
    CHECK(info.filter_generation == 2);
    CHECK(h.sm->attach_info("dyn", info)); // client session untouched
    // config entry colliding with a client-owned name is rejected, not applied
    cfg = {h.spec("x", {"QUOTE"}), h.spec("dyn", {"QUOTE"})};
    CHECK_FALSE(h.sm->reconcile_config(cfg, err));
    CHECK(err.find("client-owned") != std::string::npos);
}

TEST_CASE("SessionManager: client linger expires sessions; unknown partition and bad symbols rejected") {
    H h;
    std::string err;
    REQUIRE(h.sm->create(h.spec("c", {"TRADE"}, "client"), err));
    h.sm->client_disconnected("c", now_ns());
    h.sm->service_linger(now_ns());
    CHECK(h.sm->count() == 1);
    h.sm->service_linger(now_ns() + 100'000'000);
    CHECK(h.sm->count() == 0);

    SessionSpec bad = h.spec("b", {"TRADE"});
    bad.partitions = {"nope"};
    CHECK_FALSE(h.sm->create(bad, err));
    CHECK(err.find("unknown partition") != std::string::npos);

    SessionSpec syms = h.spec("s", {"TRADE"});
    syms.all_symbols = false;
    syms.symbols = {"1001", "ABC"};
    REQUIRE_MESSAGE(h.sm->create(syms, err), err);
    FilterSpec fs; uint64_t mask; Pipeline* g;
    REQUIRE(h.sm->resolve(syms, fs, mask, g, err));
    CHECK(fs.symbol_ids.size() == 2);
    CHECK(fs.symbol_ids[0] == h.symbols.lookup_u64(1001)); // numeric -> u64 key space, as the decoder does
    CHECK(fs.symbol_ids[1] == h.symbols.lookup("ABC"));
    CHECK(mask == 1);
    CHECK(g == &h.pipe);
    h.sm->destroy_all();
    CHECK(h.sm->count() == 0);
}
