// SPDX-License-Identifier: Apache-2.0
#pragma once

// Payload layouts for AGENT_CONTROL records (TickHeader::kind == AGENT_CONTROL).
// Shared between the agent and the header-only client. Packed little-endian PODs.
//
// INTENT: these are part of the app-facing ABI. Adding a field means bumping
// RECORD_VERSION in core/message.h. Never put pointers or strings here.

#include "subplane/core/message.h"

#include <cstdint>
#include <type_traits>

namespace subplane {

#pragma pack(push, 1)

// INTENT: events name the topic (topic_id from the site registry), never a
// partition: partitions are a publishing construct. Until the instrument
// sequence lands, the sequences in these events are the stream's own; a topic
// of several partitions has one per partition.

// GAP_DETECTED: a stream of the topic jumped forward.
struct GapEvent {
    uint16_t topic_id;
    uint16_t reserved0;
    uint32_t reserved1;
    uint64_t expected_seq;
    uint64_t received_seq;
    uint64_t detected_ns;
};

// INSTRUMENT_GAP: one instrument's sequence (the publisher's count of it in the
// topic, canonical InstrumentSeq) jumped, whichever partition carried it.
// Written to the sessions whose filter takes the symbol, before the record that
// jumped. received > expected always (a record at or below the last is a
// duplicate and dropped).
struct InstrumentGapEvent {
    uint32_t symbol_id;
    uint16_t topic_id;
    uint16_t reserved0;
    uint64_t expected_seq;
    uint64_t received_seq;
    uint64_t detected_ns;
};

// TOPIC_DEGRADED / TOPIC_RECOVERED. DEGRADED each time the count of the
// topic's degraded partitions changes and is not zero; RECOVERED when it
// reaches zero. `partitions` is how many of the topic's partitions this agent
// reads.
enum class TopicReason : uint32_t {
    NONE              = 0,
    HEARTBEAT_TIMEOUT = 1,
    SEQUENCE_GAP      = 2,
    DECODE_ERRORS     = 3,
    TRANSPORT_ERROR   = 4,
};
struct TopicStateEvent {
    uint16_t topic_id;
    uint8_t  degraded;    // partitions of the topic degraded now
    uint8_t  partitions;  // partitions of the topic this agent reads
    uint32_t reason;      // TopicReason, of the change that caused this event
    uint64_t last_seq;
    uint64_t changed_ns;
};

// TRANSPORT_DOWN / TRANSPORT_UP: a transport of the topic lost or regained
// its source. Reasons are transport-neutral (INV-015); the transport-specific
// cause is in the agent log and /partitions, not here.
enum class TransportStateReason : uint32_t {
    NONE              = 0,
    SOURCE_LOST       = 1, // remote end went away (Aeron image gone, TCP peer closed)
    LOCAL_FAILURE     = 2, // agent side failed (driver timeout, socket error)
    PUBLISHER_RESTART = 3, // on UP: a different publisher instance is the source now
};
struct TransportStateEvent {
    uint16_t topic_id;
    uint16_t reserved0;
    uint32_t reason;      // TransportStateReason
    uint64_t last_seq;    // last sequence accepted before the change
    uint64_t changed_ns;
};

// FILTER_RELOADED: the session's filter table was swapped.
struct FilterReloadedEvent {
    uint32_t session_id;
    uint32_t generation;
};

// AGENT_RESTARTED: first record on every fresh ring.
struct AgentRestartedEvent {
    uint64_t epoch;
    uint64_t prev_epoch;  // 0 if unknown
};

// SESSION_CLOSING: writer is shutting this ring down; readers should detach.
struct SessionClosingEvent {
    uint32_t session_id;
    uint32_t reason;      // 0 shutdown, 1 destroyed, 2 reload
};

// SYMBOL_MAPPED: a new symbol id was interned.
struct SymbolMappedEvent {
    uint32_t symbol_id;
    uint16_t venue_id;
    uint8_t  name_len;
    uint8_t  reserved0;
    char     name[24];    // not NUL-terminated if name_len == 24
};

// RECAP_PENDING / RECAP_BEGIN / RECAP_COMPLETE / RECAP_FAILED: the recap of
// one symbol for a session that asked for recap. Written only to the rings of
// the sessions waiting on that symbol. For one symbol a waiting session sees
// PENDING, then on success BEGIN, the snapshot records (FLAG_SNAPSHOT_ORIGIN,
// sequence = as_of_seq), COMPLETE, then live data above as_of_seq; each
// failure is a FAILED and the agent asks again.
enum class RecapEventStatus : uint8_t {
    OK            = 0,
    TIMEOUT       = 1,
    REJECTED      = 2, // the snapshot service refused (RECAP_REJECT)
    TOO_OLD       = 3, // the snapshot is older than the messages the agent holds
    HOLD_OVERFLOW = 4, // the agent ran out of room for the live messages it holds
    GAP_WHILE_HELD = 5, // reserved: a gap while pending is reported as SYMBOL_STALE{GAP}
    IDENTITY      = 6, // the snapshot came from another publisher instance (planned)
    SEND_FAILED   = 7, // the request could not be sent upstream
    REPLY_TOO_LARGE = 8, // the reply exceeds the partition's recap.reply_slab_bytes
};
struct RecapEvent {
    uint64_t request_id;   // 0 on PENDING (no request yet)
    uint32_t symbol_id;
    uint16_t topic_id;
    uint8_t  status;       // RecapEventStatus; 0 except on FAILED
    uint8_t  reserved0;
    uint32_t attempt;      // 1 for the first request, then +1 per retry; 0 on PENDING
    uint32_t record_count; // BEGIN: records in the reply; COMPLETE: records written to this ring
    uint64_t as_of_seq;    // S, on BEGIN and COMPLETE
};

// SYMBOL_STALE: the session's view of one symbol is no longer exact, and the
// agent has started a recap of it. Written only to recap sessions, once for
// each symbol of theirs on the partition. RECAP_PENDING follows for a symbol
// that was live; one already pending stays pending under a renewed request.
enum class SymbolStaleReason : uint16_t {
    GAP               = 1, // the partition's sequence jumped forward
    TRANSPORT_DOWN    = 2, // reserved: a transport going down is not a trigger by itself
    PUBLISHER_RESTART = 3, // TRANSPORT_UP{PUBLISHER_RESTART}
    SEQUENCE_RESET    = 4, // BUS_SEQ_RESET, or a backward jump beyond dup_window
    INSTRUMENT_GAP    = 5, // this symbol's instrument sequence jumped (INSTRUMENT_GAP); only this symbol
};
struct SymbolStaleEvent {
    uint32_t symbol_id;
    uint16_t topic_id;
    uint16_t reason;       // SymbolStaleReason
    uint64_t last_seq;     // the partition's last sequence before the change
    uint64_t changed_ns;
};

#pragma pack(pop)

static_assert(sizeof(GapEvent) == 32);
static_assert(sizeof(InstrumentGapEvent) == 32);
static_assert(sizeof(TopicStateEvent) == 24);
static_assert(sizeof(TransportStateEvent) == 24);
static_assert(sizeof(FilterReloadedEvent) == 8);
static_assert(sizeof(AgentRestartedEvent) == 16);
static_assert(sizeof(SessionClosingEvent) == 8);
static_assert(sizeof(SymbolMappedEvent) == 32);
static_assert(sizeof(RecapEvent) == 32);
static_assert(sizeof(SymbolStaleEvent) == 24);
static_assert(std::is_trivially_copyable_v<GapEvent>);
static_assert(std::is_trivially_copyable_v<InstrumentGapEvent>);
static_assert(std::is_trivially_copyable_v<SymbolMappedEvent>);

} // namespace subplane
