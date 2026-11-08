// SPDX-License-Identifier: Apache-2.0
#pragma once

// subplane app SDK: zero-copy reader for a subplane shared-memory ring.
// Header-only, C++20, libc only. Never throws, never allocates on poll().
//
// Usage:
//
//   #include <subplane/client/shm_reader.h>
//   using namespace subplane;
//
//   ShmReader rd;
//   std::string err;
//   if (!rd.open("/dev/shm/subplane.agent1.trades.ring", &err)) { /* log err */ }
//   rd.seek_to_head();                        // live tail; or keep pos 0 for history
//   RecordView rv;
//   for (;;) {
//       switch (rd.poll(rv)) {
//       case ReaderStatus::Ok:
//           if (is_data(*rv.hdr)) decode(rv.hdr->symbol_id, rv.payload, rv.payload_len);
//           if (!rv.valid()) discard_last_decode(); // writer lapped us mid-decode
//           break;
//       case ReaderStatus::Empty:    if (!rd.writer_alive(1'000'000'000, now_ns())) reattach(); break;
//       case ReaderStatus::Overrun:  resnapshot(); break;   // skipped to head; data lost
//       case ReaderStatus::Closed:   reattach(); break;     // agent restarted / session gone
//       default: break;
//       }
//   }
//
// Zero-copy contract: RecordView points into the shared mapping. The writer
// never waits for readers, so a slow reader can be overwritten while it looks
// at a record. Decode first, then call RecordView::valid(); if it returns
// false, whatever was decoded from that view may be torn and must be dropped.
// Treat that like Overrun (data was lost); the next poll() either resyncs
// via Overrun or continues with the next still-intact record.
//
// Reporting: open() also claims one of the ring's reader slots (ring_layout.h)
// and, about every 64 polls, writes the reader's position, heartbeat and
// overruns there, so the agent can show each app's lag and liveness with no
// app code. The name is SUBPLANE_APP_NAME (or ReaderOptions::name). When the
// agent switches latency tracing on, sampled records also record
// now - ingest_ts into the slot's histogram (one clock read per sampled
// record). No slot (an older agent, all 64 taken, or no write permission on
// the ring file) changes nothing about reading; ReaderOptions::report = false
// opts out.

#include "subplane/core/message.h"
#include "subplane/shm/control_events.h"
#include "subplane/shm/ring_layout.h"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include <cstdio>
#include <cstdlib>
#include <ctime>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace subplane {

// ReaderSlot::sdk_version written by this SDK.
inline constexpr uint32_t READER_SDK_VERSION = 1;

struct ReaderOptions {
    bool             report = true; // claim a reader slot and report through it
    std::string_view name = {};     // empty: SUBPLANE_APP_NAME, else unnamed (the agent uses the unit or comm)
};

// Why a reader has no slot (ShmReader::slot_status()).
enum class SlotStatus {
    Claimed,      // reporting through a slot
    Off,          // ReaderOptions::report was false, or not open
    NoSlots,      // the ring has no slot section (an older agent)
    NoPermission, // the ring file cannot be opened read-write by this user
    Full,         // every slot is taken
    Lost,         // the agent reclaimed the slot (it thought this reader dead)
};

enum class ReaderStatus {
    Ok,             // a record was returned
    Empty,          // caught up with the writer
    Overrun,        // lapped by the writer; position resynced to head (returned once per lap)
    WriterGone,     // writer heartbeat stale (see ShmReader::liveness)
    Closed,         // ring CLOSED and fully drained, or reader not open
    LayoutMismatch, // open(): not a subplane ring or incompatible version
};

namespace detail {
// INTENT: seq_tag lives in a PROT_READ mapping written by another process.
// std::atomic_ref<const T> is not available in C++20, so the const is cast
// away; only load() is ever called, which for a lock-free 8-byte type is a
// plain aligned load and never writes the page.
inline uint64_t load_seq_tag(const uint8_t* rec) noexcept {
    return std::atomic_ref<uint64_t>(*const_cast<uint64_t*>(reinterpret_cast<const uint64_t*>(rec)))
        .load(std::memory_order_acquire);
}
} // namespace detail

struct RecordView {
    const TickHeader* hdr         = nullptr;
    const uint8_t*    payload     = nullptr;
    uint32_t          payload_len = 0;  // validated snapshot; prefer this over hdr->payload_len
    uint64_t          pos         = 0;  // absolute ring position of the record

    // Internal: where to re-check the writer's progress.
    const RingHeader* ring        = nullptr;
    uint64_t          window      = 0;

    std::span<const uint8_t> bytes() const noexcept { return {payload, payload_len}; }

    // True iff the writer has provably not started overwriting this record.
    // Call AFTER consuming hdr/payload.
    bool valid() const noexcept {
        if (ring == nullptr) return false;
        // INTENT: acquire fence orders the consumer's plain loads of the
        // record (done before this call) before the head load below. Pairs
        // with the writer's release fence ahead of its record stores: if any
        // of those loads saw an overwriting store, this head load sees a head
        // past the safe window. Without it the head load could be satisfied
        // early and validate a torn decode.
        std::atomic_thread_fence(std::memory_order_acquire);
        const uint64_t h = ring->head.load(std::memory_order_relaxed);
        return h - pos <= window;
    }
};

class ShmReader {
public:
    ShmReader() = default;
    ShmReader(const ShmReader&) = delete;
    ShmReader& operator=(const ShmReader&) = delete;
    ~ShmReader() { close(); }

    // Cold. Maps the ring read-only and validates it. Position starts at 0
    // (oldest possible record; poll() reports Overrun if that is already
    // gone). Use seek_to_head() for a live tail. Claims a reader slot unless
    // opts.report is false; a missing slot is never an error.
    bool open(std::string_view path, std::string* err = nullptr) noexcept { return open(path, err, ReaderOptions{}); }

    bool open(std::string_view path, std::string* err, const ReaderOptions& opts) noexcept {
        close();
        const std::string p(path);
        const int fd = ::open(p.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) return fail(ReaderStatus::Closed, err, "open failed: " + p);
        struct stat st {};
        if (::fstat(fd, &st) != 0 || st.st_size < static_cast<off_t>(RING_HEADER_BYTES)) {
            ::close(fd);
            return fail(ReaderStatus::LayoutMismatch, err, "file too small for a ring header: " + p);
        }
        const auto size = static_cast<uint64_t>(st.st_size);
        void* m = ::mmap(nullptr, static_cast<size_t>(size), PROT_READ, MAP_SHARED, fd, 0);
        ::close(fd); // INTENT: the mapping keeps the file alive; no fd needed afterwards.
        if (m == MAP_FAILED) return fail(ReaderStatus::Closed, err, "mmap failed: " + p);

        const auto* h = static_cast<const RingHeader*>(m);
        const char* why = nullptr;
        if (h->magic != RING_MAGIC) why = "bad magic (not a subplane ring)";
        else if (h->layout_version != RING_LAYOUT_VERSION) why = "ring layout_version mismatch";
        else if (h->record_version != RECORD_VERSION) why = "ring record_version mismatch";
        else if (!is_pow2(h->capacity) || h->capacity < RING_MIN_CAPACITY) why = "bad capacity";
        else if (size < RING_HEADER_BYTES + h->capacity) why = "file shorter than header + capacity";
        else if (h->max_payload == 0 || h->max_payload > h->capacity / 4) why = "bad max_payload";
        if (why != nullptr) {
            ::munmap(m, static_cast<size_t>(size));
            return fail(ReaderStatus::LayoutMismatch, err, std::string(why) + ": " + p);
        }

        base_        = static_cast<const uint8_t*>(m);
        map_bytes_   = size;
        hdr_         = h;
        data_        = base_ + RING_HEADER_BYTES;
        capacity_    = h->capacity;
        mask_        = h->capacity - 1;
        max_payload_ = h->max_payload;
        window_      = ring_safe_window(h->capacity, h->max_payload);
        pos_         = 0;
        overruns_    = 0;
        records_     = 0;
        open_status_ = ReaderStatus::Ok;
        if (opts.report) claim_slot(p, size, opts.name);
        return true;
    }

    // Hot. Returns the next record (Ok), or why there is none. PAD records
    // are skipped transparently. Never blocks.
    ReaderStatus poll(RecordView& out) noexcept {
        if (hdr_ == nullptr) return ReaderStatus::Closed;
        // INTENT: reporting costs one branch and an increment per poll; the
        // clock read and the slot stores happen once per 64 polls.
        if (slot_ != nullptr && (++ticks_ & (kReportEvery - 1)) == 0) report();
        for (;;) {
            // INTENT: acquire pairs with the writer's head.store(release):
            // every record below h, including its seq_tag, is visible.
            const uint64_t h = hdr_->head.load(std::memory_order_acquire);
            if (h == pos_) {
                // INTENT: the writer stores CLOSED (release) after its last
                // head store. Seeing CLOSED with acquire therefore makes the
                // final head visible, so the re-load below is exact: only
                // "CLOSED and still nothing past pos" is end-of-stream. Using
                // the first head load alone could drop records published
                // between that load and close().
                if (hdr_->state.load(std::memory_order_acquire) == RING_STATE_CLOSED &&
                    hdr_->head.load(std::memory_order_acquire) == pos_)
                    return ReaderStatus::Closed;
                return ReaderStatus::Empty;
            }
            // Also catches pos_ > h (bad seek): the unsigned difference wraps huge.
            if (h - pos_ > window_) return overrun(h);

            const uint8_t* rec = data_ + (pos_ & mask_);
            if (detail::load_seq_tag(rec) != pos_) return overrun(h); // lapped (pos_ < h: it was published)

            const auto* th = reinterpret_cast<const TickHeader*>(rec);
            const uint32_t plen = th->payload_len;
            const uint8_t  kind = th->kind;
            const uint64_t its  = trace_ ? th->ingest_ts : 0; // validated with the other loads below
            // INTENT: same validation as RecordView::valid(): the two plain
            // loads above may race with an overwriting writer; the acquire
            // fence + head re-check proves they did not before we act on
            // them (e.g. jump by a garbage PAD length).
            std::atomic_thread_fence(std::memory_order_acquire);
            const uint64_t h2 = hdr_->head.load(std::memory_order_relaxed);
            if (h2 - pos_ > window_) return overrun(h2);

            const uint64_t rb = record_bytes(plen);
            if (kind == static_cast<uint8_t>(Kind::PAD)) {
                if ((pos_ & mask_) + rb != capacity_) return overrun(h2); // corrupt; resync
                pos_ += rb;
                continue;
            }
            if (plen > max_payload_ || (pos_ & mask_) + rb > capacity_) return overrun(h2); // corrupt

            out.hdr         = th;
            out.payload     = rec + sizeof(TickHeader);
            out.payload_len = plen;
            out.pos         = pos_;
            out.ring        = hdr_;
            out.window      = window_;
            pos_ += rb;
            ++records_;
            if (trace_ && --sample_left_ == 0) { sample_left_ = sample_every_; record_latency(its); }
            return ReaderStatus::Ok;
        }
    }

    uint64_t head() const noexcept {
        return hdr_ ? hdr_->head.load(std::memory_order_acquire) : 0;
    }
    uint64_t pos() const noexcept { return pos_; }
    // Positions must be record starts previously seen via RecordView::pos or
    // head(); anything else is detected by poll() as Overrun and resynced.
    void seek(uint64_t p) noexcept { pos_ = p; }
    void seek_to_head() noexcept { pos_ = head(); }
    uint64_t lag() const noexcept { return head() - pos_; }
    uint64_t overruns() const noexcept { return overruns_; }

    // True iff the ring is ACTIVE and the writer heartbeated within stale_ns.
    bool writer_alive(uint64_t stale_ns, uint64_t now_ns) const noexcept {
        if (hdr_ == nullptr) return false;
        if (hdr_->state.load(std::memory_order_acquire) != RING_STATE_ACTIVE) return false;
        const uint64_t hb = hdr_->heartbeat_ns.load(std::memory_order_relaxed);
        return now_ns < hb || now_ns - hb < stale_ns;
    }
    // Cold-path summary: Ok, Closed (ring CLOSED / not open) or WriterGone (stale heartbeat).
    ReaderStatus liveness(uint64_t stale_ns, uint64_t now_ns) const noexcept {
        if (hdr_ == nullptr || state() == RING_STATE_CLOSED) return ReaderStatus::Closed;
        return writer_alive(stale_ns, now_ns) ? ReaderStatus::Ok : ReaderStatus::WriterGone;
    }

    uint32_t state() const noexcept { return hdr_ ? hdr_->state.load(std::memory_order_acquire) : RING_STATE_CLOSED; }
    uint64_t epoch() const noexcept { return hdr_ ? hdr_->epoch : 0; }
    // Precondition: is_open().
    const RingHeader& header() const noexcept { return *hdr_; }
    bool is_open() const noexcept { return hdr_ != nullptr; }
    // Why the last open() failed (Ok if it succeeded).
    ReaderStatus open_status() const noexcept { return open_status_; }
    // Whether this reader reports through a slot, and why not.
    SlotStatus slot_status() const noexcept { return slot_status_; }
    // True while the agent has latency tracing on for this reader.
    bool tracing() const noexcept { return trace_; }
    // Cold: report now (position, heartbeat, flags) instead of waiting for the next 64th poll.
    void report_now() noexcept { if (slot_ != nullptr) report(); }

    void close() noexcept {
        release_slot();
        if (base_ != nullptr) ::munmap(const_cast<uint8_t*>(base_), static_cast<size_t>(map_bytes_));
        base_ = nullptr;
        hdr_  = nullptr;
        data_ = nullptr;
        pos_  = 0;
    }

private:
    static constexpr uint64_t kReportEvery = 64; // power of two

    static uint64_t realtime_ns() noexcept {
        timespec ts{};
        ::clock_gettime(CLOCK_REALTIME, &ts);
        return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ull + static_cast<uint64_t>(ts.tv_nsec);
    }

    // /proc/self/stat field 22: with the pid, it names this process uniquely.
    static uint64_t self_start_time() noexcept {
        char buf[1024];
        FILE* f = std::fopen("/proc/self/stat", "re");
        if (f == nullptr) return 0;
        const size_t n = std::fread(buf, 1, sizeof buf - 1, f);
        std::fclose(f);
        buf[n] = '\0';
        const char* p = std::strrchr(buf, ')'); // comm may contain spaces and ')'
        if (p == nullptr) return 0;
        ++p;
        for (int field = 2; field < 22 && *p != '\0'; ++p) if (*p == ' ') ++field; // p is now just past the 21st space
        return std::strtoull(p, nullptr, 10);
    }

    // Cold. Maps the slot section read-write and claims a free slot.
    void claim_slot(const std::string& path, uint64_t file_size, std::string_view name) noexcept {
        slot_status_ = SlotStatus::NoSlots;
        const uint64_t off = hdr_->readers_offset;
        const uint64_t n = hdr_->reader_slots;
        if (off == 0 || n == 0 || hdr_->reader_slot_bytes != READER_SLOT_BYTES || off != RING_HEADER_BYTES + capacity_ ||
            n > 4096 || file_size < off + n * READER_SLOT_BYTES)
            return;
        const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
        if (fd < 0) { slot_status_ = SlotStatus::NoPermission; return; }
        const size_t len = static_cast<size_t>(n * READER_SLOT_BYTES);
        // INTENT: only the slot pages are writable. A stray write through the
        // header or data mapping still faults, as it did before slots existed.
        void* m = ::mmap(nullptr, len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, static_cast<off_t>(off));
        ::close(fd);
        if (m == MAP_FAILED) { slot_status_ = SlotStatus::NoPermission; return; }
        auto* slots = static_cast<ReaderSlot*>(m);
        pid_ = static_cast<uint32_t>(::getpid());
        for (uint64_t i = 0; i < n; ++i) {
            uint32_t expected = READER_SLOT_FREE;
            if (!slots[i].owner_pid.compare_exchange_strong(expected, pid_, std::memory_order_acq_rel)) continue;
            ReaderSlot& s = slots[i];
            s.identity_ready.store(0, std::memory_order_relaxed);
            s.start_time = self_start_time();
            s.uid = static_cast<uint32_t>(::getuid());
            s.sdk_version = READER_SDK_VERSION;
            s.capabilities = READER_CAP_TRACE_LATENCY;
            s.reserved = 0;
            std::string_view nm = name;
            if (nm.empty()) { const char* e = std::getenv("SUBPLANE_APP_NAME"); if (e != nullptr) nm = e; }
            std::memset(s.name, 0, sizeof s.name);
            std::memcpy(s.name, nm.data(), nm.size() < sizeof s.name ? nm.size() : sizeof s.name - 1);
            s.overruns.store(0, std::memory_order_relaxed);
            s.records.store(0, std::memory_order_relaxed);
            s.active_flags.store(0, std::memory_order_relaxed);
            for (auto& b : s.lat_buckets) b.store(0, std::memory_order_relaxed);
            s.lat_sum_ns.store(0, std::memory_order_relaxed);
            s.lat_count.store(0, std::memory_order_relaxed);
            // INTENT: release: the agent reads the identity only after seeing 1 (acquire).
            s.identity_ready.store(1, std::memory_order_release);
            slot_map_ = m;
            slot_map_bytes_ = len;
            slot_ = &s;
            slot_status_ = SlotStatus::Claimed;
            report();
            return;
        }
        ::munmap(m, len);
        slot_status_ = SlotStatus::Full;
    }

    // Writes position, heartbeat and counters; picks up the agent's flags.
    void report() noexcept {
        // INTENT: if the agent reclaimed this slot (it judged us dead) another
        // reader may own it now: stop writing to it at once.
        if (slot_->owner_pid.load(std::memory_order_relaxed) != pid_) {
            slot_ = nullptr;
            trace_ = false;
            slot_status_ = SlotStatus::Lost;
            return;
        }
        slot_->read_pos.store(pos_, std::memory_order_relaxed);
        slot_->overruns.store(overruns_, std::memory_order_relaxed);
        slot_->records.store(records_, std::memory_order_relaxed);
        slot_->heartbeat_ns.store(realtime_ns(), std::memory_order_relaxed);
        const uint32_t f = (hdr_->reader_flags.load(std::memory_order_relaxed) | slot_->flags.load(std::memory_order_relaxed)) &
                           READER_FLAG_TRACE_LATENCY;
        uint32_t every = slot_->sample_every.load(std::memory_order_relaxed);
        if (every == 0) every = hdr_->reader_sample_every.load(std::memory_order_relaxed);
        if (every == 0) every = 1;
        const bool trace = (f & READER_FLAG_TRACE_LATENCY) != 0;
        if (trace && (!trace_ || every != sample_every_)) { sample_every_ = every; sample_left_ = every; }
        trace_ = trace;
        slot_->active_flags.store(f, std::memory_order_relaxed);
    }

    // One traced record: now - ingest_ts into the slot histogram. Single
    // writer (this reader), so relaxed load + store, no RMW.
    void record_latency(uint64_t ingest_ts) noexcept {
        if (slot_ == nullptr || ingest_ts == 0) return;
        const uint64_t now = realtime_ns();
        const uint64_t d = now > ingest_ts ? now - ingest_ts : 0;
        auto add = [](std::atomic<uint64_t>& a, uint64_t v) { a.store(a.load(std::memory_order_relaxed) + v, std::memory_order_relaxed); };
        add(slot_->lat_buckets[reader_lat_bucket(d)], 1);
        add(slot_->lat_sum_ns, d);
        add(slot_->lat_count, 1);
    }

    void release_slot() noexcept {
        if (slot_ != nullptr) {
            ReaderSlot& s = *slot_;
            s.identity_ready.store(0, std::memory_order_relaxed);
            s.read_pos.store(0, std::memory_order_relaxed);
            s.heartbeat_ns.store(0, std::memory_order_relaxed);
            s.active_flags.store(0, std::memory_order_relaxed);
            // INTENT: release: a later claimer that wins the CAS sees our resets.
            uint32_t mine = pid_;
            s.owner_pid.compare_exchange_strong(mine, READER_SLOT_FREE, std::memory_order_acq_rel);
        }
        if (slot_map_ != nullptr) ::munmap(slot_map_, slot_map_bytes_);
        slot_ = nullptr;
        slot_map_ = nullptr;
        slot_map_bytes_ = 0;
        trace_ = false;
        slot_status_ = SlotStatus::Off;
    }

    ReaderStatus overrun(uint64_t h) noexcept {
        ++overruns_;
        pos_ = h; // head is always a record boundary
        return ReaderStatus::Overrun;
    }

    bool fail(ReaderStatus s, std::string* err, std::string msg) noexcept {
        open_status_ = s;
        if (err != nullptr) *err = msg;
        return false;
    }

    const RingHeader* hdr_         = nullptr;
    const uint8_t*    data_        = nullptr;
    uint64_t          pos_         = 0;
    uint64_t          mask_        = 0;
    uint64_t          capacity_    = 0;
    uint64_t          window_      = 0;
    uint32_t          max_payload_ = 0;
    uint64_t          overruns_    = 0;
    const uint8_t*    base_        = nullptr;
    uint64_t          map_bytes_   = 0;
    ReaderStatus      open_status_ = ReaderStatus::Closed;

    // Reporting (cold except ticks_, trace_ and the sampling counters).
    ReaderSlot*       slot_          = nullptr;
    uint64_t          ticks_         = 0;
    uint64_t          records_       = 0;
    bool              trace_         = false;
    uint32_t          sample_every_  = 1;
    uint32_t          sample_left_   = 1;
    uint32_t          pid_           = 0;
    void*             slot_map_      = nullptr;
    size_t            slot_map_bytes_ = 0;
    SlotStatus        slot_status_   = SlotStatus::Off;
};

} // namespace subplane
