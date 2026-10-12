#pragma once

// On-disk / in-memory layout of a flashtick shared-memory ring.
//
// Shared by the agent (fanout/ring_writer.h) and the header-only app client
// (client/shm_reader.h). libc + <atomic> only: this file is part of the
// app-facing ABI. Any change to RingHeader or to the record framing rules
// below bumps RING_LAYOUT_VERSION.
//
// File = [RingHeader: 4096 B][data region: capacity B]
//
// Data region: a variable-length byte ring addressed by an absolute,
// monotonically increasing byte position (never masked in the header; the
// offset in the data region is `pos & (capacity - 1)`). Each record is
//   TickHeader (64 B) + payload, padded up to a multiple of RECORD_ALIGN.
// A record never straddles the end of the data region: if the next record
// would cross it, the writer first emits a PAD record (kind PAD, msg_type
// ControlCode::PAD) covering the remaining bytes, with
//   payload_len = bytes_skipped - sizeof(TickHeader)
// so a reader can jump over it with record_bytes(payload_len).
//
// Publication protocol (single writer, any number of read-only readers):
//   writer: plain-store header fields (except seq_tag) + payload;
//           TickHeader::seq_tag.store(pos, release)   (atomic_ref)
//           head.store(pos + record_bytes, release)
//   reader: h = head.load(acquire); pos == h -> empty;
//           h - pos > ring_safe_window(...) -> overrun;
//           seq_tag.load(acquire) != pos -> lapped (overrun);
//           read fields; fence(acquire); re-load head; re-check window.

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace flashtick {

inline constexpr uint64_t RING_MAGIC          = 0x3130474E49525446ull; // "FTRING01" little-endian
inline constexpr uint32_t RING_LAYOUT_VERSION = 1;
inline constexpr uint64_t RING_HEADER_BYTES   = 4096;
inline constexpr uint64_t RECORD_ALIGN        = 64;
inline constexpr uint64_t RING_MIN_CAPACITY   = 64 * 1024;

// RingHeader::state values.
inline constexpr uint32_t RING_STATE_INIT   = 0;
inline constexpr uint32_t RING_STATE_ACTIVE = 1;
inline constexpr uint32_t RING_STATE_CLOSED = 2;

// INTENT: readers map the file PROT_READ and load these atomics from a
// different process. That is only sound if they are lock-free (a lock-based
// atomic would try to write its lock into a read-only page, and the lock would
// not be shared across processes anyway).
static_assert(std::atomic<uint64_t>::is_always_lock_free, "ring atomics must be lock-free");
static_assert(std::atomic<uint32_t>::is_always_lock_free, "ring atomics must be lock-free");
static_assert(sizeof(std::atomic<uint64_t>) == 8 && sizeof(std::atomic<uint32_t>) == 4,
              "ring atomics must have the layout of their value type");

// INTENT: one cache line per independently-written field group so the
// writer's head stores, heartbeat stores and stat stores never false-share
// with each other, and readers spinning on `head` only take a miss when it
// actually changes. The immutable block is written once at creation and
// never again, so sharing its second line with nothing hot is free.
struct alignas(4096) RingHeader {
    // immutable after creation (72 bytes: bytes 0..71, spilling into line 1)
    uint64_t magic;            // RING_MAGIC
    uint32_t layout_version;   // RING_LAYOUT_VERSION
    uint32_t record_version;   // RECORD_VERSION (core/message.h)
    uint64_t epoch;            // agent start time ns; changes on every restart
    uint64_t capacity;         // data region bytes, power of two
    uint32_t max_payload;      // largest payload the writer will accept
    uint32_t writer_pid;
    char     session_name[32]; // NUL-terminated

    // next write position (absolute bytes, never masked)
    alignas(64) std::atomic<uint64_t> head;

    // writer liveness, ns since epoch (CLOCK_REALTIME)
    alignas(64) std::atomic<uint64_t> heartbeat_ns;

    // RING_STATE_*
    alignas(64) std::atomic<uint32_t> state;

    // writer statistics (relaxed, informational only)
    alignas(64) std::atomic<uint64_t> records_written; // non-PAD records
    std::atomic<uint64_t> bytes_written;               // bytes advanced, including PAD
    std::atomic<uint64_t> control_records;             // BUS_ADMIN + AGENT_CONTROL records
};

static_assert(sizeof(RingHeader) == RING_HEADER_BYTES, "RingHeader must be exactly one page");
static_assert(offsetof(RingHeader, magic) == 0);
static_assert(offsetof(RingHeader, layout_version) == 8);
static_assert(offsetof(RingHeader, record_version) == 12);
static_assert(offsetof(RingHeader, epoch) == 16);
static_assert(offsetof(RingHeader, capacity) == 24);
static_assert(offsetof(RingHeader, max_payload) == 32);
static_assert(offsetof(RingHeader, writer_pid) == 36);
static_assert(offsetof(RingHeader, session_name) == 40);
// The immutable block is 72 bytes, so it spans lines 0-1 and head starts on
// the next line boundary.
static_assert(offsetof(RingHeader, head) == 128);
static_assert(offsetof(RingHeader, heartbeat_ns) == 192);
static_assert(offsetof(RingHeader, state) == 256);
static_assert(offsetof(RingHeader, records_written) == 320);
static_assert(offsetof(RingHeader, bytes_written) == 328);
static_assert(offsetof(RingHeader, control_records) == 336);

// Bytes occupied on the ring by a record with `payload_len` payload bytes.
// 64-byte TickHeader + payload, rounded up to RECORD_ALIGN.
inline constexpr uint64_t record_bytes(uint32_t payload_len) noexcept {
    return (64u + static_cast<uint64_t>(payload_len) + (RECORD_ALIGN - 1)) & ~(RECORD_ALIGN - 1);
}

inline constexpr bool is_pow2(uint64_t v) noexcept { return v != 0 && (v & (v - 1)) == 0; }

// Largest distance `head - pos` at which the record at `pos` is guaranteed
// intact.
//
// INTENT: the writer plain-stores a record's bytes at [head, head + record)
// BEFORE it advances head, so while head == H it may already be overwriting
// any byte at absolute position < H + record_bytes(max_payload). The record at
// `pos` is clobbered once the writer touches pos + capacity, so it is only
// provably intact while H + record_bytes(max_payload) <= pos + capacity. Using
// `head - pos <= capacity` instead (the naive bound) would let a reader accept
// a header/payload that is being overwritten by the in-flight record. The
// writer publishes a wrap PAD (which is always smaller than the record that
// caused it) with its own head store before writing the record, so at most
// one record is ever in flight.
inline constexpr uint64_t ring_safe_window(uint64_t capacity, uint32_t max_payload) noexcept {
    return capacity - record_bytes(max_payload);
}

} // namespace flashtick
