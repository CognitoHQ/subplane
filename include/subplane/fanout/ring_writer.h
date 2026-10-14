#pragma once

// Single-writer side of the shared-memory ring (see shm/ring_layout.h).
//
// INTENT: concrete, header-only, no virtuals. publish() is on the pipeline hot
// path: no heap, no mutex, no syscalls, never waits for readers. Readers map
// the file PROT_READ, so a slow or dead reader can never stall the writer; it
// simply gets lapped and detects that itself. Exactly ONE thread may call the
// non-const methods of a given RingWriter.

#include "subplane/core/message.h"
#include "subplane/shm/control_events.h"
#include "subplane/shm/ring_layout.h"

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <new>
#include <string>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace subplane {

struct RingConfig {
    uint64_t    capacity_bytes = 0; // data region bytes; power of two, >= 64 KiB
    uint32_t    max_payload    = 0; // <= capacity_bytes / 4
    std::string session_name;       // <= 31 chars
    bool        huge_pages     = false;
};

class RingWriter {
public:
    RingWriter() = default;
    RingWriter(const RingWriter&) = delete;
    RingWriter& operator=(const RingWriter&) = delete;
    ~RingWriter() { release(); }

    // Cold. Creates `path + ".tmp"`, sizes and maps it, fills the header, marks
    // it ACTIVE and atomically renames it to `path` (replacing any previous
    // ring file there; readers holding the old mapping keep it).
    bool open(const RingConfig& cfg, const std::string& path, uint64_t epoch, std::string& err) {
        if (base_ != nullptr) { err = "ring already open"; return false; }
        if (!is_pow2(cfg.capacity_bytes) || cfg.capacity_bytes < RING_MIN_CAPACITY) {
            err = "capacity must be a power of two >= 65536";
            return false;
        }
        if (cfg.max_payload == 0 || cfg.max_payload > cfg.capacity_bytes / 4) {
            err = "max_payload must be in [1, capacity/4]";
            return false;
        }
        if (cfg.session_name.size() >= sizeof(RingHeader::session_name)) {
            err = "session_name longer than 31 bytes";
            return false;
        }

        const std::string tmp = path + ".tmp";
        // INTENT: a .tmp left behind by a crashed agent would otherwise make
        // O_EXCL fail forever. O_EXCL still catches two live writers racing
        // for the same path between this unlink and the open.
        ::unlink(tmp.c_str());
        const int fd = ::open(tmp.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0644);
        if (fd < 0) { err = "open " + tmp + ": " + std::strerror(errno); return false; }

        const uint64_t total = RING_HEADER_BYTES + cfg.capacity_bytes;
        if (::ftruncate(fd, static_cast<off_t>(total)) != 0) {
            err = "ftruncate " + tmp + ": " + std::strerror(errno);
            ::close(fd); ::unlink(tmp.c_str());
            return false;
        }
        // INTENT: MAP_POPULATE pre-faults every page now, on the cold path, so
        // the first lap of publish() does not take page faults (each one is a
        // multi-microsecond kernel entry on the hot path).
        void* p = ::mmap(nullptr, static_cast<size_t>(total), PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_POPULATE, fd, 0);
        if (p == MAP_FAILED) {
            err = "mmap " + tmp + ": " + std::strerror(errno);
            ::close(fd); ::unlink(tmp.c_str());
            return false;
        }
        if (cfg.huge_pages) {
            // Best effort: tmpfs only honours this when shmem_enabled allows it.
            (void)::madvise(p, static_cast<size_t>(total), MADV_HUGEPAGE);
        }

        auto* h = new (p) RingHeader{};
        h->magic          = RING_MAGIC;
        h->layout_version = RING_LAYOUT_VERSION;
        h->record_version = RECORD_VERSION;
        h->epoch          = epoch;
        h->capacity       = cfg.capacity_bytes;
        h->max_payload    = cfg.max_payload;
        h->writer_pid     = static_cast<uint32_t>(::getpid());
        std::memcpy(h->session_name, cfg.session_name.data(), cfg.session_name.size());
        h->session_name[cfg.session_name.size()] = '\0';
        h->head.store(0, std::memory_order_relaxed);
        h->heartbeat_ns.store(0, std::memory_order_relaxed);
        h->records_written.store(0, std::memory_order_relaxed);
        h->bytes_written.store(0, std::memory_order_relaxed);
        h->control_records.store(0, std::memory_order_relaxed);
        // INTENT: release so a reader that observes ACTIVE (acquire) also sees
        // the immutable line-0 fields. In practice the rename below is the
        // real publication point (a reader cannot open the file before it),
        // but the ordering keeps the header self-consistent regardless.
        h->state.store(RING_STATE_ACTIVE, std::memory_order_release);

        if (::rename(tmp.c_str(), path.c_str()) != 0) {
            err = "rename " + tmp + " -> " + path + ": " + std::strerror(errno);
            ::munmap(p, static_cast<size_t>(total));
            ::close(fd); ::unlink(tmp.c_str());
            return false;
        }

        fd_          = fd;
        base_        = static_cast<uint8_t*>(p);
        map_bytes_   = total;
        hdr_         = h;
        data_        = base_ + RING_HEADER_BYTES;
        capacity_    = cfg.capacity_bytes;
        mask_        = cfg.capacity_bytes - 1;
        max_payload_ = cfg.max_payload;
        head_        = 0;
        records_     = 0;
        bytes_       = 0;
        controls_    = 0;
        active_      = true;
        path_        = path;
        return true;
    }

    // Hot. Copies `hdr` (except seq_tag and payload_len, which the ring owns)
    // and `len` payload bytes onto the ring. Returns false if the ring is not
    // ACTIVE or len > max_payload. Never blocks, never allocates.
    bool publish(const TickHeader& hdr, const uint8_t* bytes, uint32_t len) noexcept {
        if (!active_ || len > max_payload_ || (len != 0 && bytes == nullptr)) return false;
        const uint64_t rec = record_bytes(len);
        const uint64_t off = head_ & mask_;
        if (off + rec > capacity_) {
            // Remaining bytes are a multiple of 64 and >= 64 (offsets are
            // 64-aligned), so a PAD header always fits.
            const uint64_t skip = capacity_ - off;
            const TickHeader pad = make_control_header(ControlCode::PAD, 0, 0,
                                                       static_cast<uint32_t>(skip - sizeof(TickHeader)));
            write_record(pad, pad.payload_len, nullptr, 0, skip);
        }
        write_record(hdr, len, bytes, len, rec);
        ++records_;
        const uint8_t k = hdr.kind;
        if (k == static_cast<uint8_t>(Kind::BUS_ADMIN) || k == static_cast<uint8_t>(Kind::AGENT_CONTROL)) ++controls_;
        // INTENT: relaxed, and plain store of a writer-local shadow instead of
        // fetch_add: there is a single writer, so no RMW (lock-prefixed on
        // x86) is needed, and these counters order nothing.
        hdr_->records_written.store(records_, std::memory_order_relaxed);
        hdr_->bytes_written.store(bytes_, std::memory_order_relaxed);
        hdr_->control_records.store(controls_, std::memory_order_relaxed);
        return true;
    }

    // Hot. Agent control / bus admin record with a POD body.
    bool publish_control(ControlCode code, uint16_t partition, uint64_t ingest_ts,
                         const void* body, uint32_t len) noexcept {
        const TickHeader h = make_control_header(code, partition, ingest_ts, len);
        return publish(h, static_cast<const uint8_t*>(body), len);
    }

    // ~1 kHz from the pipeline loop.
    void heartbeat(uint64_t now_ns) noexcept {
        if (hdr_ == nullptr) return;
        // INTENT: relaxed; liveness is advisory and orders no other data.
        hdr_->heartbeat_ns.store(now_ns, std::memory_order_relaxed);
    }

    // Marks the ring CLOSED. Further publish() calls fail. Idempotent.
    void close() noexcept {
        if (hdr_ == nullptr || !active_) return;
        active_ = false;
        // INTENT: release so a reader that sees CLOSED (acquire) after
        // draining to head has also seen every record published before
        // close; it can then treat "empty + CLOSED" as end-of-stream.
        hdr_->state.store(RING_STATE_CLOSED, std::memory_order_release);
    }

    const std::string& path() const noexcept { return path_; }
    // Writer thread only: the non-atomic shadow.
    uint64_t head() const noexcept { return head_; }
    // INTENT: any thread (the control thread reads it for SNAPSHOT replies).
    // head_ is a plain field the pipeline thread writes, so reading it from
    // another thread is a data race; the published head is the atomic.
    uint64_t published_head() const noexcept {
        return hdr_ ? hdr_->head.load(std::memory_order_acquire) : 0;
    }
    bool is_open() const noexcept { return base_ != nullptr; }
    uint64_t capacity() const noexcept { return capacity_; }
    uint32_t max_payload() const noexcept { return max_payload_; }

    // Cold. The agent decides when a ring file goes away; existing mappings
    // stay readable after unlink.
    static bool unlink_ring(const std::string& path) { return ::unlink(path.c_str()) == 0; }

private:
    // Writes one record at head_ and publishes it. `hdr_len` goes into
    // TickHeader::payload_len; `copy_len` payload bytes are copied. They differ
    // only for PAD (hdr_len = skipped bytes - 64, nothing copied). `rec` is the
    // number of ring bytes the record occupies.
    void write_record(const TickHeader& src, uint32_t hdr_len, const uint8_t* bytes, uint32_t copy_len,
                      uint64_t rec) noexcept {
        const uint64_t pos = head_;
        uint8_t* slot = data_ + (pos & mask_);

        // INTENT: orders the previous head.store(release) before the plain
        // stores below. Readers validate a record by (load bytes; fence
        // acquire; load head; check head - pos <= safe window). That check is
        // only sound if a reader that observes ANY byte of this new record
        // also observes head >= pos, i.e. the head store must become visible
        // before these stores. A release store alone does not keep later
        // plain stores behind it (the compiler and ARM may hoist them). On
        // x86 this is a compiler barrier only; on ARM64 it is `dmb ish`.
        // Same pattern as core/seqlock.h, with plain memcpy stores because
        // the payload is read zero-copy by foreign processes.
        std::atomic_thread_fence(std::memory_order_release);

        TickHeader h = src;
        h.payload_len = hdr_len;
        // INTENT: copy everything EXCEPT seq_tag (bytes 0..7). seq_tag still
        // holds the previous lap's position, so a reader looking at this slot
        // cannot mistake the half-written record for a published one.
        static_assert(offsetof(TickHeader, seq_tag) == 0 && sizeof(h.seq_tag) == 8);
        std::memcpy(slot + 8, reinterpret_cast<const uint8_t*>(&h) + 8, sizeof(TickHeader) - 8);
        if (copy_len != 0) std::memcpy(slot + sizeof(TickHeader), bytes, copy_len);

        // INTENT: release — publication flag. A reader that loads seq_tag ==
        // pos with acquire sees the header fields and payload above. Relaxed
        // here would let it read a stale header under a fresh tag.
        std::atomic_ref<uint64_t>(*reinterpret_cast<uint64_t*>(slot)).store(pos, std::memory_order_release);

        head_ = pos + rec;
        bytes_ += rec;
        // INTENT: release — readers load head with acquire and then trust
        // every record below it (including its seq_tag). Relaxed would let a
        // reader see the new head before the record's bytes.
        hdr_->head.store(head_, std::memory_order_release);
    }

    void release() noexcept {
        close();
        if (base_ != nullptr) ::munmap(base_, static_cast<size_t>(map_bytes_));
        if (fd_ >= 0) ::close(fd_);
        base_ = nullptr;
        hdr_  = nullptr;
        data_ = nullptr;
        fd_   = -1;
    }

    // Hot fields first.
    RingHeader* hdr_         = nullptr;
    uint8_t*    data_        = nullptr;
    uint64_t    head_        = 0; // writer-local shadow of hdr_->head
    uint64_t    mask_        = 0;
    uint64_t    capacity_    = 0;
    uint32_t    max_payload_ = 0;
    bool        active_      = false;
    uint64_t    records_     = 0;
    uint64_t    bytes_       = 0;
    uint64_t    controls_    = 0;

    uint8_t*    base_        = nullptr;
    uint64_t    map_bytes_   = 0;
    int         fd_          = -1;
    std::string path_;
};

} // namespace subplane
