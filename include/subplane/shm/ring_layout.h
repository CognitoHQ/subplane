// SPDX-License-Identifier: Apache-2.0
#pragma once

// On-disk / in-memory layout of a subplane shared-memory ring.
//
// Shared by the agent (fanout/ring_writer.h) and the header-only app client
// (client/shm_reader.h). libc + <atomic> only: this file is part of the
// app-facing ABI. Any change to RingHeader or to the record framing rules
// below bumps RING_LAYOUT_VERSION.
//
// File = [RingHeader: 4096 B][data region: capacity B][reader slots: 64 x 512 B]
//
// Reader slots (optional, after the data region so the data offset never
// moves): each app's reader claims one and reports its identity, read
// position, heartbeat and, when the agent asks, its read latency. The header
// says where they are (readers_offset; 0 = a ring without slots, from an older
// agent). Readers map the header and data read-only and may write only their
// own slot; the writer's hot path never reads slots (INV-011).
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

namespace subplane {

inline constexpr uint64_t RING_MAGIC          = 0x3130474E49525053ull; // "SPRING01" little-endian
inline constexpr uint32_t RING_LAYOUT_VERSION = 1;
inline constexpr uint64_t RING_HEADER_BYTES   = 4096;
inline constexpr uint64_t RECORD_ALIGN        = 64;
inline constexpr uint64_t RING_MIN_CAPACITY   = 64 * 1024;

inline constexpr uint32_t READER_SLOTS        = 64;
inline constexpr uint64_t READER_SLOT_BYTES   = 512;
inline constexpr uint64_t READER_SECTION_BYTES = READER_SLOTS * READER_SLOT_BYTES; // 32 KiB, whole pages
inline constexpr uint32_t READER_LAT_BUCKETS  = 38;  // power-of-two ns buckets; the last is the overflow

// Flags, agent to readers (RingHeader::reader_flags for every reader, ReaderSlot::flags for one).
// Advisory: a reader applies them within about 64 polls; capabilities say which it honours.
inline constexpr uint32_t READER_FLAG_TRACE_LATENCY = 1u << 0; // record now - ingest_ts per (sampled) record
// Capabilities, reader to agent (ReaderSlot::capabilities).
inline constexpr uint32_t READER_CAP_TRACE_LATENCY  = 1u << 0;

// ReaderSlot::owner_pid values besides a pid.
inline constexpr uint32_t READER_SLOT_FREE       = 0;
inline constexpr uint32_t READER_SLOT_RECLAIMING = 0xFFFFFFFFu; // the agent is freeing a dead reader's slot

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
    uint64_t readers_offset;   // byte offset of the reader slots, 0 = none (an older agent)
    uint32_t reader_slots;     // READER_SLOTS
    uint32_t reader_slot_bytes; // READER_SLOT_BYTES

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

    // agent -> every reader (cold writes from the agent's API thread)
    alignas(64) std::atomic<uint32_t> reader_flags;    // READER_FLAG_*
    std::atomic<uint32_t> reader_sample_every;         // trace 1 record in N; 0 or 1 = every record
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
static_assert(offsetof(RingHeader, readers_offset) == 72);
static_assert(offsetof(RingHeader, reader_slots) == 80);
static_assert(offsetof(RingHeader, reader_slot_bytes) == 84);
// The immutable block is 88 bytes, so it spans lines 0-1 and head starts on
// the next line boundary. Bytes 72..87 were zero in rings without slots.
static_assert(offsetof(RingHeader, head) == 128);
static_assert(offsetof(RingHeader, heartbeat_ns) == 192);
static_assert(offsetof(RingHeader, state) == 256);
static_assert(offsetof(RingHeader, records_written) == 320);
static_assert(offsetof(RingHeader, bytes_written) == 328);
static_assert(offsetof(RingHeader, control_records) == 336);
static_assert(offsetof(RingHeader, reader_flags) == 384);
static_assert(offsetof(RingHeader, reader_sample_every) == 388);

// One reader's slot. Each line has one writer, so agent and app never
// false-share. Contents are untrusted by the agent: used only for reporting,
// bounds-checked, never for anything that affects data delivery.
//
// Claim: CAS owner_pid FREE -> pid, write identity, identity_ready = 1
// (release). Release: identity_ready = 0, zero lines 1..7, owner_pid = FREE
// (release). The agent reclaims a slot whose owner is dead (pid gone, or a
// different start_time) AND whose heartbeat is stale: CAS pid -> RECLAIMING,
// zero, FREE.
struct alignas(64) ReaderSlot {
    // line 0: agent -> this reader
    std::atomic<uint32_t> flags;          // READER_FLAG_*, OR'ed with RingHeader::reader_flags
    std::atomic<uint32_t> sample_every;   // overrides RingHeader::reader_sample_every when nonzero

    // line 1: identity (the reader, once per claim)
    alignas(64) std::atomic<uint32_t> owner_pid; // READER_SLOT_FREE, a pid, or READER_SLOT_RECLAIMING
    std::atomic<uint32_t> identity_ready;        // 1 once the fields below are written
    uint64_t start_time;                         // /proc/self/stat field 22: (pid, start_time) is unique
    uint32_t uid;
    uint32_t sdk_version;                        // READER_SDK_VERSION of the reader's SDK
    uint32_t capabilities;                       // READER_CAP_*
    uint32_t reserved;
    char     name[32];                           // SUBPLANE_APP_NAME, NUL-terminated, may be empty

    // line 2: progress (the reader, about every 64 polls)
    alignas(64) std::atomic<uint64_t> read_pos;  // the reader's ring position: lag = head - read_pos
    std::atomic<uint64_t> heartbeat_ns;          // CLOCK_REALTIME of the last report
    std::atomic<uint64_t> overruns;
    std::atomic<uint64_t> records;               // records returned by poll()
    std::atomic<uint32_t> active_flags;          // the flags the reader currently applies

    // lines 3..7: read latency, now - ingest_ts per traced record. Bucket m
    // holds [2^m, 2^(m+1)) ns (0 ns counts in bucket 0); the last bucket holds
    // everything from 2^(READER_LAT_BUCKETS-1) ns (about 137 s) up.
    alignas(64) std::atomic<uint64_t> lat_buckets[READER_LAT_BUCKETS];
    std::atomic<uint64_t> lat_sum_ns;
    std::atomic<uint64_t> lat_count;
};

static_assert(sizeof(ReaderSlot) == READER_SLOT_BYTES, "a reader slot is exactly 512 bytes");
static_assert(offsetof(ReaderSlot, flags) == 0);
static_assert(offsetof(ReaderSlot, sample_every) == 4);
static_assert(offsetof(ReaderSlot, owner_pid) == 64);
static_assert(offsetof(ReaderSlot, identity_ready) == 68);
static_assert(offsetof(ReaderSlot, start_time) == 72);
static_assert(offsetof(ReaderSlot, uid) == 80);
static_assert(offsetof(ReaderSlot, sdk_version) == 84);
static_assert(offsetof(ReaderSlot, capabilities) == 88);
static_assert(offsetof(ReaderSlot, name) == 96);
static_assert(offsetof(ReaderSlot, read_pos) == 128);
static_assert(offsetof(ReaderSlot, heartbeat_ns) == 136);
static_assert(offsetof(ReaderSlot, overruns) == 144);
static_assert(offsetof(ReaderSlot, records) == 152);
static_assert(offsetof(ReaderSlot, active_flags) == 160);
static_assert(offsetof(ReaderSlot, lat_buckets) == 192);
static_assert(offsetof(ReaderSlot, lat_sum_ns) == 192 + 8 * READER_LAT_BUCKETS);
static_assert(offsetof(ReaderSlot, lat_count) == 504);
static_assert(READER_SECTION_BYTES % 4096 == 0, "the slot section is whole pages, mapped on its own");

// The latency bucket of a value in ns.
inline constexpr uint32_t reader_lat_bucket(uint64_t ns) noexcept {
    if (ns == 0) return 0;
    const uint32_t m = 63u - static_cast<uint32_t>(__builtin_clzll(ns));
    return m < READER_LAT_BUCKETS ? m : READER_LAT_BUCKETS - 1;
}

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

} // namespace subplane
