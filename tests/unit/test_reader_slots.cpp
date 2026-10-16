// Reader slots (shm/ring_layout.h): the SDK claims, reports through and
// releases a slot; tracing follows the agent's flags; readers keep reading
// when there is no slot; the data mapping stays read-only.
#include <doctest.h>

#include "subplane/client/shm_reader.h"
#include "subplane/fanout/ring_writer.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <ctime>
#include <memory>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

using namespace subplane;

namespace {

struct Ring {
    std::string path;
    RingWriter w;
    explicit Ring(uint32_t mode = 0644) {
        static std::atomic<int> n{0};
        path = "/dev/shm/subplane_slots_" + std::to_string(::getpid()) + "_" + std::to_string(n.fetch_add(1)) + ".ring";
        RingConfig c;
        c.capacity_bytes = 1u << 16;
        c.max_payload = 1024;
        c.session_name = "slots";
        c.mode = mode;
        std::string err;
        REQUIRE_MESSAGE(w.open(c, path, 9, err), err);
    }
    ~Ring() { ::unlink(path.c_str()); }
    ReaderSlot& slot(uint32_t i) { return w.reader_slots()[i]; }
    void publish(uint64_t ingest_ts) {
        TickHeader h{};
        h.ingest_ts = ingest_ts;
        h.kind = static_cast<uint8_t>(Kind::DATA);
        const uint8_t body[16] = {};
        REQUIRE(w.publish(h, body, sizeof body));
    }
};

uint64_t realtime() {
    timespec ts{};
    ::clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ull + static_cast<uint64_t>(ts.tv_nsec);
}

void drain(ShmReader& rd) {
    RecordView v;
    while (rd.poll(v) == ReaderStatus::Ok) {}
}

} // namespace

TEST_CASE("reader slots: the layout leaves the data region where it was") {
    Ring r;
    ShmReader rd;
    REQUIRE(rd.open(r.path));
    CHECK(rd.header().readers_offset == RING_HEADER_BYTES + (1u << 16));
    CHECK(rd.header().reader_slots == READER_SLOTS);
    CHECK(rd.header().reader_slot_bytes == READER_SLOT_BYTES);
    CHECK(rd.header().layout_version == 1);
}

TEST_CASE("reader slots: open claims one, reports identity and progress, close releases it") {
    Ring r;
    ShmReader a, b;
    ReaderOptions o;
    o.name = "mm-engine";
    REQUIRE(a.open(r.path, nullptr, o));
    REQUIRE(b.open(r.path));
    CHECK(a.slot_status() == SlotStatus::Claimed);
    CHECK(b.slot_status() == SlotStatus::Claimed);
    ReaderSlot& s = r.slot(0);
    CHECK(s.owner_pid.load() == static_cast<uint32_t>(::getpid()));
    CHECK(s.identity_ready.load() == 1);
    CHECK(std::string(s.name) == "mm-engine");
    CHECK(s.sdk_version == READER_SDK_VERSION);
    CHECK(s.capabilities == READER_CAP_TRACE_LATENCY);
    CHECK(s.uid == static_cast<uint32_t>(::getuid()));
    CHECK(s.start_time != 0);
    CHECK(s.heartbeat_ns.load() != 0); // reported at claim
    CHECK(r.slot(1).owner_pid.load() == static_cast<uint32_t>(::getpid()));

    for (int i = 0; i < 10; ++i) r.publish(realtime());
    drain(a);
    a.report_now();
    CHECK(s.read_pos.load() == r.w.published_head());
    CHECK(s.records.load() == 10);
    CHECK(s.lat_count.load() == 0); // tracing is off

    a.close();
    CHECK(s.owner_pid.load() == READER_SLOT_FREE);
    CHECK(s.identity_ready.load() == 0);
    CHECK(a.slot_status() == SlotStatus::Off);
    ShmReader c;
    REQUIRE(c.open(r.path));
    CHECK(s.owner_pid.load() == static_cast<uint32_t>(::getpid())); // the freed slot is reused
}

TEST_CASE("reader slots: a full table, an old ring and no write permission all leave reading intact") {
    SUBCASE("full") {
        Ring r;
        std::vector<std::unique_ptr<ShmReader>> rs;
        for (uint32_t i = 0; i < READER_SLOTS; ++i) {
            rs.push_back(std::make_unique<ShmReader>());
            REQUIRE(rs.back()->open(r.path));
            REQUIRE(rs.back()->slot_status() == SlotStatus::Claimed);
        }
        ShmReader extra;
        REQUIRE(extra.open(r.path));
        CHECK(extra.slot_status() == SlotStatus::Full);
        r.publish(1);
        RecordView v;
        CHECK(extra.poll(v) == ReaderStatus::Ok);
    }
    SUBCASE("a ring from an agent without slots") {
        Ring r;
        const int fd = ::open(r.path.c_str(), O_RDWR);
        REQUIRE(fd >= 0);
        const uint64_t zero = 0;
        REQUIRE(::pwrite(fd, &zero, 8, offsetof(RingHeader, readers_offset)) == 8);
        ::close(fd);
        ShmReader rd;
        REQUIRE(rd.open(r.path));
        CHECK(rd.slot_status() == SlotStatus::NoSlots);
        r.publish(1);
        RecordView v;
        CHECK(rd.poll(v) == ReaderStatus::Ok);
    }
    SUBCASE("read-only file") {
        if (::geteuid() == 0) return; // root opens anything read-write
        Ring r(0444);
        ShmReader rd;
        REQUIRE(rd.open(r.path));
        CHECK(rd.slot_status() == SlotStatus::NoPermission);
        r.publish(1);
        RecordView v;
        CHECK(rd.poll(v) == ReaderStatus::Ok);
    }
    SUBCASE("opted out") {
        Ring r;
        ShmReader rd;
        ReaderOptions o;
        o.report = false;
        REQUIRE(rd.open(r.path, nullptr, o));
        CHECK(rd.slot_status() == SlotStatus::Off);
        CHECK(r.slot(0).owner_pid.load() == READER_SLOT_FREE);
    }
}

TEST_CASE("reader slots: latency tracing follows the ring-wide and per-slot flags, with sampling") {
    Ring r;
    ShmReader rd;
    REQUIRE(rd.open(r.path));
    CHECK_FALSE(rd.tracing());
    r.w.set_reader_flags(READER_FLAG_TRACE_LATENCY, 2);
    RecordView v;
    for (int i = 0; i < 64; ++i) (void)rd.poll(v); // the flags are picked up within 64 polls
    CHECK(rd.tracing());
    ReaderSlot& s = r.slot(0);
    CHECK(s.active_flags.load() == READER_FLAG_TRACE_LATENCY);
    const uint64_t t = realtime() - 5000; // ingested 5 us ago, at least
    for (int i = 0; i < 20; ++i) r.publish(t);
    drain(rd);
    CHECK(s.lat_count.load() == 10); // 1 in 2
    CHECK(s.lat_sum_ns.load() >= 10 * 5000);
    uint64_t in_buckets = 0;
    for (const auto& b : s.lat_buckets) in_buckets += b.load();
    CHECK(in_buckets == 10);
    CHECK(s.lat_buckets[0].load() == 0); // nothing under 2 ns: they were all >= 5 us

    r.w.set_reader_flags(0, 0);
    s.flags.store(READER_FLAG_TRACE_LATENCY); // this reader only
    for (int i = 0; i < 64; ++i) (void)rd.poll(v);
    CHECK(rd.tracing());
    s.flags.store(0);
    for (int i = 0; i < 64; ++i) (void)rd.poll(v);
    CHECK_FALSE(rd.tracing());
    CHECK(s.active_flags.load() == 0);
}

TEST_CASE("reader slots: a reader whose slot was taken from it stops writing to it") {
    Ring r;
    ShmReader rd;
    REQUIRE(rd.open(r.path));
    ReaderSlot& s = r.slot(0);
    s.owner_pid.store(424242); // as if the agent reclaimed it and another reader claimed it
    s.read_pos.store(777);
    RecordView v;
    for (int i = 0; i < 64; ++i) (void)rd.poll(v);
    CHECK(rd.slot_status() == SlotStatus::Lost);
    r.publish(1);
    drain(rd);
    rd.report_now();
    CHECK(s.read_pos.load() == 777);   // untouched
    rd.close();
    CHECK(s.owner_pid.load() == 424242); // not released: it is not ours
}

TEST_CASE("reader slots: the header and data mapping stay read-only for readers") {
    Ring r;
    ShmReader rd;
    REQUIRE(rd.open(r.path));
    REQUIRE(rd.slot_status() == SlotStatus::Claimed);
    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
        // A stray write through the reader's header mapping must fault.
        std::signal(SIGSEGV, SIG_DFL); // doctest's handler would report it as a failed test
        auto* h = const_cast<RingHeader*>(&rd.header());
        h->capacity = 1;
        ::_exit(0);
    }
    int st = 0;
    REQUIRE(::waitpid(child, &st, 0) == child);
    CHECK(WIFSIGNALED(st));
    CHECK(WTERMSIG(st) == SIGSEGV);
}
