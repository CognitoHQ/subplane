#include <doctest.h>

#include "flashtick/client/shm_reader.h"
#include "flashtick/fanout/ring_writer.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

using namespace flashtick;

namespace {

std::string temp_dir() {
    if (::access("/dev/shm", W_OK) == 0) return "/dev/shm";
    const char* t = std::getenv("TMPDIR");
    return (t != nullptr && *t != '\0') ? std::string(t) : std::string("/tmp");
}

// Unique ring path per test; unlinked (with any stale .tmp) in teardown.
struct TempRing {
    std::string path;
    TempRing() {
        static std::atomic<int> counter{0};
        path = temp_dir() + "/flashtick_test_" + std::to_string(::getpid()) + "_" +
               std::to_string(counter.fetch_add(1)) + ".ring";
    }
    ~TempRing() {
        ::unlink(path.c_str());
        ::unlink((path + ".tmp").c_str());
    }
};

RingConfig cfg(uint64_t cap, uint32_t max_payload) {
    RingConfig c;
    c.capacity_bytes = cap;
    c.max_payload    = max_payload;
    c.session_name   = "test";
    return c;
}

TickHeader data_header(uint64_t seq) {
    TickHeader h{};
    h.sequence    = seq;
    h.ingest_ts   = 1000 + seq;
    h.symbol_id   = static_cast<uint32_t>(seq % 97);
    h.kind        = static_cast<uint8_t>(Kind::DATA);
    h.msg_type    = static_cast<uint8_t>(MsgType::TRADE);
    h.wire_format = static_cast<uint8_t>(WireFormat::SBE);
    return h;
}

// Deterministic payload: length and bytes derived from seq.
uint32_t payload_len_for(uint64_t seq, uint32_t max) { return static_cast<uint32_t>((seq * 2654435761u) % (max + 1)); }
void fill_payload(std::vector<uint8_t>& buf, uint64_t seq, uint32_t len) {
    buf.resize(len);
    for (uint32_t i = 0; i < len; ++i) buf[i] = static_cast<uint8_t>((seq * 31u + i) & 0xFFu);
}
bool payload_matches(const RecordView& rv, uint64_t seq) {
    for (uint32_t i = 0; i < rv.payload_len; ++i)
        if (rv.payload[i] != static_cast<uint8_t>((seq * 31u + i) & 0xFFu)) return false;
    return true;
}

bool publish_seq(RingWriter& w, uint64_t seq, uint32_t len) {
    std::vector<uint8_t> buf;
    fill_payload(buf, seq, len);
    return w.publish(data_header(seq), buf.data(), len);
}

void poke(const std::string& path, off_t off, const void* bytes, size_t n) {
    const int fd = ::open(path.c_str(), O_RDWR);
    REQUIRE(fd >= 0);
    REQUIRE(::pwrite(fd, bytes, n, off) == static_cast<ssize_t>(n));
    ::close(fd);
}

} // namespace

TEST_CASE("ring: open/publish/poll roundtrip") {
    TempRing tr;
    RingWriter w;
    std::string err;
    REQUIRE(w.open(cfg(1 << 20, 4096), tr.path, 777, err));
    CHECK(w.is_open());
    CHECK(w.path() == tr.path);
    CHECK(::access((tr.path + ".tmp").c_str(), F_OK) != 0); // renamed away

    ShmReader r;
    REQUIRE(r.open(tr.path, &err));
    CHECK(r.open_status() == ReaderStatus::Ok);
    CHECK(r.header().capacity == (1u << 20));
    CHECK(std::string(r.header().session_name) == "test");
    CHECK(r.header().writer_pid == static_cast<uint32_t>(::getpid()));

    RecordView rv;
    CHECK(r.poll(rv) == ReaderStatus::Empty);

    const uint8_t payload[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    REQUIRE(w.publish(data_header(42), payload, sizeof(payload)));
    AgentRestartedEvent ev{777, 0};
    REQUIRE(w.publish_control(ControlCode::AGENT_RESTARTED, 3, 555, &ev, sizeof(ev)));
    REQUIRE(w.publish(data_header(43), nullptr, 0));

    REQUIRE(r.poll(rv) == ReaderStatus::Ok);
    CHECK(rv.pos == 0);
    CHECK(rv.hdr->sequence == 42);
    REQUIRE(rv.payload_len == sizeof(payload));
    CHECK(std::memcmp(rv.payload, payload, sizeof(payload)) == 0);
    CHECK(rv.bytes().size() == sizeof(payload));
    CHECK(rv.valid());

    REQUIRE(r.poll(rv) == ReaderStatus::Ok);
    CHECK(rv.pos == record_bytes(sizeof(payload)));
    CHECK(is_control(*rv.hdr));
    CHECK(rv.hdr->kind == static_cast<uint8_t>(Kind::AGENT_CONTROL));
    CHECK(rv.hdr->msg_type == static_cast<uint8_t>(ControlCode::AGENT_RESTARTED));
    CHECK(rv.hdr->partition_id == 3);
    CHECK(rv.hdr->ingest_ts == 555);
    REQUIRE(rv.payload_len == sizeof(ev));
    AgentRestartedEvent got{};
    std::memcpy(&got, rv.payload, sizeof(got));
    CHECK(got.epoch == 777);

    REQUIRE(r.poll(rv) == ReaderStatus::Ok);
    CHECK(rv.hdr->sequence == 43);
    CHECK(rv.payload_len == 0);

    CHECK(r.poll(rv) == ReaderStatus::Empty);
    CHECK(r.lag() == 0);
    CHECK(r.pos() == w.head());
    CHECK(r.header().records_written.load() == 3);
    CHECK(r.header().control_records.load() == 1);
    CHECK(r.header().bytes_written.load() == w.head());
}

TEST_CASE("ring: header fields preserved except seq_tag and payload_len") {
    TempRing tr;
    RingWriter w;
    std::string err;
    REQUIRE(w.open(cfg(1 << 16, 1024), tr.path, 1, err));

    TickHeader in{};
    in.seq_tag        = 0xDEADBEEFull; // must be overwritten by the ring position
    in.sequence       = 0x1122334455667788ull;
    in.exchange_ts    = 123456789;
    in.ingest_ts      = 987654321;
    in.symbol_id      = 4242;
    in.payload_len    = 9999;          // must be overwritten by len
    in.template_id    = 11;
    in.schema_id      = 22;
    in.schema_version = 33;
    in.venue_id       = 44;
    in.partition_id   = 55;
    in.msg_type       = static_cast<uint8_t>(MsgType::QUOTE);
    in.kind           = static_cast<uint8_t>(Kind::DATA);
    in.wire_format    = static_cast<uint8_t>(WireFormat::SBE);
    in.flags          = FLAG_SNAPSHOT_ORIGIN;
    for (uint8_t i = 0; i < 10; ++i) in.reserved[i] = static_cast<uint8_t>(i + 1);

    const uint8_t p[5] = {9, 8, 7, 6, 5};
    REQUIRE(w.publish(in, p, 5));
    REQUIRE(w.publish(in, p, 3));

    ShmReader r;
    REQUIRE(r.open(tr.path));
    RecordView rv;
    for (uint64_t expect_pos : {uint64_t{0}, record_bytes(5)}) {
        REQUIRE(r.poll(rv) == ReaderStatus::Ok);
        CHECK(rv.hdr->seq_tag == expect_pos);
        CHECK(rv.hdr->payload_len == (expect_pos == 0 ? 5u : 3u));
        TickHeader a = *rv.hdr, b = in;
        a.seq_tag = b.seq_tag = 0;
        a.payload_len = b.payload_len = 0;
        CHECK(std::memcmp(&a, &b, sizeof(TickHeader)) == 0);
    }
}

TEST_CASE("ring: PAD at wrap, varying payloads, reader sees every record in order") {
    TempRing tr;
    RingWriter w;
    std::string err;
    constexpr uint64_t kCap = 64 * 1024;
    constexpr uint32_t kMax = 3000;
    REQUIRE(w.open(cfg(kCap, kMax), tr.path, 1, err));
    ShmReader r;
    REQUIRE(r.open(tr.path, &err));

    RecordView rv;
    uint64_t next_expected = 0;
    constexpr uint64_t kN = 20000;
    for (uint64_t seq = 0; seq < kN; ++seq) {
        REQUIRE(publish_seq(w, seq, payload_len_for(seq, kMax)));
        if (seq % 7 == 6 || seq + 1 == kN) { // read in small batches, never lapped
            ReaderStatus st;
            while ((st = r.poll(rv)) == ReaderStatus::Ok) {
                REQUIRE(rv.hdr->sequence == next_expected);
                REQUIRE(rv.payload_len == payload_len_for(next_expected, kMax));
                REQUIRE(payload_matches(rv, next_expected));
                REQUIRE(rv.valid());
                REQUIRE(rv.hdr->kind != static_cast<uint8_t>(Kind::PAD));
                ++next_expected;
            }
            REQUIRE(st == ReaderStatus::Empty);
        }
    }
    CHECK(next_expected == kN);
    CHECK(r.overruns() == 0);
    CHECK(w.head() > 100 * kCap); // wrapped many times
    // PAD bytes are counted in bytes_written but not records_written.
    CHECK(r.header().records_written.load() == kN);
    CHECK(r.header().bytes_written.load() == w.head());
}

TEST_CASE("ring: 1 writer + 4 readers see identical gap-free sequences") {
    TempRing tr;
    RingWriter w;
    std::string err;
    constexpr uint64_t kN = 200000;
    constexpr uint32_t kMax = 200;
    // Large enough to hold the whole run: no reader can be lapped.
    REQUIRE(w.open(cfg(64ull << 20, kMax), tr.path, 9, err));

    constexpr int kReaders = 4;
    std::atomic<int> ready{0};
    std::vector<uint64_t> seen(kReaders, 0);
    std::vector<int> errors(kReaders, 0);
    std::vector<std::thread> th;
    for (int i = 0; i < kReaders; ++i) {
        th.emplace_back([&, i] {
            ShmReader r;
            if (!r.open(tr.path)) { errors[static_cast<size_t>(i)]++; ready++; return; }
            ready++;
            RecordView rv;
            uint64_t expect = 0;
            for (;;) {
                const ReaderStatus st = r.poll(rv);
                if (st == ReaderStatus::Ok) {
                    if (rv.hdr->sequence != expect || rv.payload_len != payload_len_for(expect, kMax) ||
                        !payload_matches(rv, expect) || !rv.valid())
                        errors[static_cast<size_t>(i)]++;
                    ++expect;
                } else if (st == ReaderStatus::Closed) {
                    break;
                } else if (st != ReaderStatus::Empty) {
                    errors[static_cast<size_t>(i)]++;
                    break;
                }
            }
            seen[static_cast<size_t>(i)] = expect;
        });
    }
    while (ready.load() < kReaders) std::this_thread::yield();
    for (uint64_t seq = 0; seq < kN; ++seq) REQUIRE(publish_seq(w, seq, payload_len_for(seq, kMax)));
    w.close();
    for (auto& t : th) t.join();
    for (int i = 0; i < kReaders; ++i) {
        CHECK(errors[static_cast<size_t>(i)] == 0);
        CHECK(seen[static_cast<size_t>(i)] == kN);
    }
}

TEST_CASE("ring: overrun reported once, then resync to head with monotonic sequences") {
    TempRing tr;
    RingWriter w;
    std::string err;
    REQUIRE(w.open(cfg(64 * 1024, 1024), tr.path, 1, err));
    ShmReader r;
    REQUIRE(r.open(tr.path, &err));

    uint64_t seq = 0;
    for (; seq < 10000; ++seq) REQUIRE(publish_seq(w, seq, 100)); // ~1.9 MB through a 64 KiB ring
    CHECK(r.lag() > 64 * 1024);

    RecordView rv;
    CHECK(r.poll(rv) == ReaderStatus::Overrun);
    CHECK(r.overruns() == 1);
    CHECK(r.pos() == w.head());
    CHECK(r.poll(rv) == ReaderStatus::Empty);
    CHECK(r.overruns() == 1);

    const uint64_t resume = seq;
    for (; seq < resume + 50; ++seq) REQUIRE(publish_seq(w, seq, 100));
    uint64_t expect = resume;
    while (r.poll(rv) == ReaderStatus::Ok) {
        CHECK(rv.hdr->sequence == expect);
        CHECK(payload_matches(rv, expect));
        ++expect;
    }
    CHECK(expect == resume + 50);
    CHECK(r.overruns() == 1);

    // A garbage seek is treated as an overrun and resynced, never trusted.
    r.seek(w.head() - 64 * 3 + 64); // not a record start
    CHECK(r.poll(rv) == ReaderStatus::Overrun);
    CHECK(r.pos() == w.head());
    r.seek(w.head() + 4096); // ahead of the writer
    CHECK(r.poll(rv) == ReaderStatus::Overrun);
    CHECK(r.overruns() == 3);
}

TEST_CASE("ring: concurrent writer far ahead of a slow reader") {
    TempRing tr;
    RingWriter w;
    std::string err;
    REQUIRE(w.open(cfg(64 * 1024, 512), tr.path, 1, err));
    ShmReader r;
    REQUIRE(r.open(tr.path, &err));

    std::atomic<bool> done{false};
    std::thread wt([&] {
        for (uint64_t seq = 0; seq < 300000; ++seq) (void)publish_seq(w, seq, payload_len_for(seq, 512));
        w.close();
        done = true;
    });
    RecordView rv;
    uint64_t last = 0;
    bool have_last = false;
    uint64_t torn = 0;
    for (;;) {
        const ReaderStatus st = r.poll(rv);
        if (st == ReaderStatus::Closed) break;
        if (st == ReaderStatus::Overrun) { have_last = false; continue; }
        if (st != ReaderStatus::Ok) continue;
        const uint64_t s = rv.hdr->sequence;
        const bool ok_payload = rv.payload_len == payload_len_for(s, 512) && payload_matches(rv, s);
        if (!rv.valid()) { torn++; have_last = false; continue; } // decode discarded = data loss, as for Overrun
        CHECK(ok_payload);
        if (have_last) CHECK(s == last + 1); // within one lap: no gaps
        last = s;
        have_last = true;
        if (s % 64 == 0) std::this_thread::sleep_for(std::chrono::microseconds(20)); // be slow
    }
    wt.join();
    CHECK(done.load());
    MESSAGE("overruns=" << r.overruns() << " torn=" << torn);
}

TEST_CASE("ring: lapped RecordView becomes invalid") {
    TempRing tr;
    RingWriter w;
    std::string err;
    constexpr uint64_t kCap = 64 * 1024;
    REQUIRE(w.open(cfg(kCap, 1024), tr.path, 1, err));
    ShmReader r;
    REQUIRE(r.open(tr.path, &err));

    REQUIRE(publish_seq(w, 0, 100));
    RecordView rv;
    REQUIRE(r.poll(rv) == ReaderStatus::Ok);
    CHECK(rv.valid());
    for (uint64_t s = 1; s < 50; ++s) REQUIRE(publish_seq(w, s, 100)); // well within the window
    CHECK(rv.valid());
    CHECK(rv.hdr->sequence == 0);
    for (uint64_t s = 50; s < 1000; ++s) REQUIRE(publish_seq(w, s, 100)); // laps it
    CHECK_FALSE(rv.valid());
    CHECK(rv.hdr->seq_tag != rv.pos); // slot reused
    CHECK_FALSE(RecordView{}.valid());
}

TEST_CASE("ring: CLOSED detection, heartbeat and writer_alive") {
    TempRing tr;
    RingWriter w;
    std::string err;
    REQUIRE(w.open(cfg(64 * 1024, 1024), tr.path, 31337, err));
    ShmReader r;
    REQUIRE(r.open(tr.path, &err));
    CHECK(r.epoch() == 31337);
    CHECK(r.state() == RING_STATE_ACTIVE);

    constexpr uint64_t kSec = 1'000'000'000ull;
    w.heartbeat(100 * kSec);
    CHECK(r.writer_alive(kSec, 100 * kSec + 10));
    CHECK(r.liveness(kSec, 100 * kSec + 10) == ReaderStatus::Ok);
    CHECK_FALSE(r.writer_alive(kSec, 102 * kSec));
    CHECK(r.liveness(kSec, 102 * kSec) == ReaderStatus::WriterGone);

    REQUIRE(publish_seq(w, 1, 10));
    w.close();
    CHECK_FALSE(publish_seq(w, 2, 10)); // rejected after close
    CHECK(r.state() == RING_STATE_CLOSED);
    CHECK_FALSE(r.writer_alive(kSec, 100 * kSec + 10));
    CHECK(r.liveness(kSec, 100 * kSec + 10) == ReaderStatus::Closed);

    RecordView rv;
    REQUIRE(r.poll(rv) == ReaderStatus::Ok); // drains what was published before close
    CHECK(rv.hdr->sequence == 1);
    CHECK(r.poll(rv) == ReaderStatus::Closed);

    // Mapping survives unlink; the agent decides when files go away.
    CHECK(RingWriter::unlink_ring(tr.path));
    CHECK(r.epoch() == 31337);
}

TEST_CASE("ring: open validation and oversize payload") {
    TempRing tr;
    RingWriter w;
    std::string err;
    CHECK_FALSE(w.open(cfg(100000, 1024), tr.path, 1, err));    // not a power of two
    CHECK_FALSE(w.open(cfg(32 * 1024, 1024), tr.path, 1, err)); // below minimum
    CHECK_FALSE(w.open(cfg(64 * 1024, 64 * 1024), tr.path, 1, err)); // max_payload > cap/4
    RingConfig longname = cfg(64 * 1024, 1024);
    longname.session_name = std::string(32, 'x');
    CHECK_FALSE(w.open(longname, tr.path, 1, err));
    CHECK_FALSE(w.is_open());

    REQUIRE(w.open(cfg(64 * 1024, 1024), tr.path, 1, err));
    std::vector<uint8_t> big(1025, 0xAB);
    CHECK_FALSE(w.publish(data_header(1), big.data(), 1025));
    CHECK(w.publish(data_header(1), big.data(), 1024));
    CHECK(w.head() == record_bytes(1024));

    ShmReader r;
    CHECK_FALSE(r.open(tr.path + ".missing", &err));
    CHECK_FALSE(r.is_open());
    RecordView rv;
    CHECK(r.poll(rv) == ReaderStatus::Closed);
}

TEST_CASE("ring: LayoutMismatch on bad magic") {
    TempRing tr;
    RingWriter w;
    std::string err;
    REQUIRE(w.open(cfg(64 * 1024, 1024), tr.path, 1, err));
    const uint64_t bad = 0x0123456789ABCDEFull;
    poke(tr.path, 0, &bad, sizeof(bad));
    ShmReader r;
    err.clear();
    CHECK_FALSE(r.open(tr.path, &err));
    CHECK(r.open_status() == ReaderStatus::LayoutMismatch);
    CHECK_FALSE(err.empty());
}

TEST_CASE("ring: reader refuses record_version mismatch") {
    TempRing tr;
    RingWriter w;
    std::string err;
    REQUIRE(w.open(cfg(64 * 1024, 1024), tr.path, 1, err));
    const uint32_t v = RECORD_VERSION + 1;
    poke(tr.path, static_cast<off_t>(offsetof(RingHeader, record_version)), &v, sizeof(v));
    ShmReader r;
    CHECK_FALSE(r.open(tr.path, &err));
    CHECK(r.open_status() == ReaderStatus::LayoutMismatch);
    CHECK(err.find("record_version") != std::string::npos);

    const uint32_t lv = RING_LAYOUT_VERSION + 1;
    const uint32_t rv_ok = RECORD_VERSION;
    poke(tr.path, static_cast<off_t>(offsetof(RingHeader, record_version)), &rv_ok, sizeof(rv_ok));
    poke(tr.path, static_cast<off_t>(offsetof(RingHeader, layout_version)), &lv, sizeof(lv));
    CHECK_FALSE(r.open(tr.path, &err));
    CHECK(r.open_status() == ReaderStatus::LayoutMismatch);
}
