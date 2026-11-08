#include <doctest.h>

#include "fake_transport.h"
#include "subplane/client/shm_reader.h"
#include "subplane/pipeline/pipeline.h"
#include "subplane/record/pcapng.h"
#include "subplane/record/sha256.h"
#include "subplane/shm/control_events.h"

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iterator>
#include <map>
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

const char* kTapYaml = R"(
name: eq
transport: { type: udp_multicast, group: "239.1.2.3", port: 31000, kernel_timestamps: true }
framing: { sequence: message.SeqNum }
decoder:
  type: sbe_routing
  schema: sample_md.xml
  fields: { symbol: SecurityID, sequence: SeqNum }
  messages:
    - { template: 1, msg_type: TRADE }
tap: { capacity_mb: 1, max_payload: 256 }
)";

struct TapHarness {
    SymbolTable symbols{1024};
    Lvc lvc;
    Partition part;
    FakeTransport* fake = nullptr;
    Pipeline pipe;
    std::string dir;

    explicit TapHarness(const char* yaml = kTapYaml) : pipe("g0", 4242, symbols, lvc) {
        char tmpl[] = "/dev/shm/subplane-taptest-XXXXXX";
        REQUIRE(mkdtemp(tmpl) != nullptr);
        dir = tmpl;
        std::string err;
        REQUIRE(lvc.init(1024, {static_cast<uint8_t>(MsgType::TRADE)}, 256, err));
        PartitionConfig cfg;
        REQUIRE_MESSAGE(PartitionConfig::parse(YAML::Load(yaml), 0, data_dir(), cfg, err), err);
        auto ft = std::make_unique<FakeTransport>(8, 1500);
        fake = ft.get();
        REQUIRE_MESSAGE(part.build(cfg, err, std::move(ft)), err);
        REQUIRE(part.open(err));
        TapSetup ts;
        ts.ring_dir = dir;
        ts.agent_name = "a1";
        ts.agent_version = "9.9-test";
        ts.epoch = 4242;
        REQUIRE_MESSAGE(part.open_tap(ts, err), err);
        pipe.add_partition(&part);
    }
    ~TapHarness() {
        part.close();
        if (!part.tap_path().empty()) ::unlink(part.tap_path().c_str());
        if (!part.tap_context_path().empty()) ::unlink(part.tap_context_path().c_str());
        ::rmdir(dir.c_str());
    }
};

std::string read_file(const std::string& p) {
    std::ifstream f(p, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}

} // namespace

TEST_CASE("tap: every received unit lands in the tap ring as it arrived, malformed ones too") {
    TapHarness h;
    REQUIRE(h.part.tap != nullptr);
    CHECK(h.part.tap_path() == h.dir + "/subplane.a1.tap.eq.ring");

    const std::vector<uint8_t> junk = {0xde, 0xad, 0xbe, 0xef, 0x01}; // not SBE: the decoder rejects it
    const std::vector<uint8_t> big(300, 0x55);                     // larger than tap.max_payload 256
    h.fake->up();
    h.fake->push(junk, 1000);
    h.fake->push(big, 1001);
    h.fake->push({0x01, 0x02}, 1002);
    h.pipe.run_once();
    h.fake->down(TransportReason::NONE); // reported by the next poll
    h.pipe.run_once();

    const auto c = h.part.counters.snapshot();
    CHECK(c.packets == 3);
    CHECK(c.tap_records == 2);
    CHECK(c.tap_dropped == 1);

    ShmReader rd;
    std::string err;
    REQUIRE_MESSAGE(rd.open(h.part.tap_path(), &err), err);
    struct Rec { uint8_t kind; uint8_t code; uint64_t seq; uint64_t ts; std::vector<uint8_t> bytes; };
    std::vector<Rec> recs;
    RecordView v;
    while (rd.poll(v) == ReaderStatus::Ok) {
        recs.push_back({v.hdr->kind, v.hdr->msg_type, v.hdr->sequence, v.hdr->ingest_ts,
                        std::vector<uint8_t>(v.payload, v.payload + v.payload_len)});
    }
    // UP (a control record), the two units that fit, with the oversized one a gap in
    // `sequence` (1, then 3), then DOWN.
    REQUIRE(recs.size() == 4);
    CHECK(recs[0].kind == static_cast<uint8_t>(Kind::AGENT_CONTROL));
    CHECK(recs[0].code == static_cast<uint8_t>(ControlCode::TRANSPORT_UP));
    CHECK(recs[1].kind == static_cast<uint8_t>(Kind::RAW));
    CHECK(recs[1].seq == 1);
    CHECK(recs[1].ts == 1000);
    CHECK(recs[1].bytes == junk);
    CHECK(recs[2].seq == 3);
    CHECK(recs[2].ts == 1002);
    CHECK(recs[2].bytes == std::vector<uint8_t>{0x01, 0x02});
    CHECK(recs[3].code == static_cast<uint8_t>(ControlCode::TRANSPORT_DOWN));
}

TEST_CASE("tap: the context file says how to read the recording") {
    TapHarness h;
    const std::string ctx = read_file(h.part.tap_context_path());
    REQUIRE(!ctx.empty());
    std::map<record::SpRecTag, std::string> t;
    CHECK(record::tlv_each(reinterpret_cast<const uint8_t*>(ctx.data()), uint32_t(ctx.size()),
                           [&](record::SpRecTag tag, const uint8_t* v, uint32_t n) { t[tag].assign(reinterpret_cast<const char*>(v), n); }));
    CHECK(t[record::SpRecTag::AGENT_NAME] == "a1");
    CHECK(t[record::SpRecTag::AGENT_VERSION] == "9.9-test");
    CHECK(t[record::SpRecTag::PARTITION] == "eq");
    CHECK(t[record::SpRecTag::TRANSPORT_TYPE] == "udp_multicast");
    CHECK(t[record::SpRecTag::TS_SOURCE] == "kernel");
    uint64_t epoch = 0;
    REQUIRE(t[record::SpRecTag::AGENT_EPOCH].size() == 8);
    std::memcpy(&epoch, t[record::SpRecTag::AGENT_EPOCH].data(), 8);
    CHECK(epoch == 4242);

    // The partition's YAML round-trips through a parse; the schema is the file the decoder loaded.
    const std::string yaml = t[record::SpRecTag::PARTITION_YAML];
    const YAML::Node n = YAML::Load(yaml);
    CHECK(n["name"].as<std::string>() == "eq");
    CHECK(n["transport"]["port"].as<int>() == 31000);
    const std::string xml = t[record::SpRecTag::SCHEMA_XML];
    CHECK(xml == read_file(data_dir() + "/sample_md.xml"));
    const auto ys = record::sha256(yaml), xs = record::sha256(xml);
    CHECK(t[record::SpRecTag::PARTITION_SHA256] == std::string(ys.begin(), ys.end()));
    CHECK(t[record::SpRecTag::SCHEMA_SHA256] == std::string(xs.begin(), xs.end()));

    // 239.1.2.3:31000 for the synthetic UDP header.
    REQUIRE(t[record::SpRecTag::WRAP].size() == 8);
    uint32_t ip = 0;
    uint16_t port = 0;
    std::memcpy(&ip, t[record::SpRecTag::WRAP].data(), 4);
    std::memcpy(&port, t[record::SpRecTag::WRAP].data() + 4, 2);
    CHECK(ip == 0xEF010203u);
    CHECK(port == 31000);
}

TEST_CASE("tap: off by default; bad tap blocks are refused") {
    const char* no_tap = R"(
name: p
transport: { type: udp, group: "", port: 1 }
decoder: { type: sbe_routing, schema: sample_md.xml, messages: [ { template: 1, msg_type: TRADE } ] }
)";
    PartitionConfig cfg;
    std::string err;
    REQUIRE(PartitionConfig::parse(YAML::Load(no_tap), 0, data_dir(), cfg, err));
    CHECK_FALSE(cfg.tap);
    Partition p;
    REQUIRE_MESSAGE(p.build(cfg, err, std::make_unique<FakeTransport>()), err);
    TapSetup ts;
    ts.ring_dir = "/nonexistent";
    CHECK(p.open_tap(ts, err)); // nothing to open
    CHECK(p.tap == nullptr);
    CHECK(p.tap_path().empty());
    CHECK(p.schema_xml().empty()); // only kept when a recording needs it

    for (const char* bad : {"tap: on", "tap: { capacity_mb: 3 }", "tap: { capacity_mb: 1, max_payload: 1000000 }"}) {
        const std::string y = std::string(no_tap) + bad + "\n";
        CHECK_FALSE(PartitionConfig::parse(YAML::Load(y), 0, data_dir(), cfg, err));
        CHECK(err.find("tap") != std::string::npos);
    }
}

TEST_CASE("sha256: FIPS 180-4 test vectors") {
    using record::sha256;
    using record::to_hex;
    CHECK(to_hex(sha256("")) == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    CHECK(to_hex(sha256("abc")) == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    CHECK(to_hex(sha256("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")) ==
          "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
    const std::string million(1'000'000, 'a');
    CHECK(to_hex(sha256(million)) == "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
}
