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

// GAP_DETECTED: sequence jumped forward on a partition.
struct GapEvent {
    uint16_t partition_id;
    uint16_t reserved0;
    uint32_t reserved1;
    uint64_t expected_seq;
    uint64_t received_seq;
    uint64_t detected_ns;
};

// PARTITION_DEGRADED / PARTITION_RECOVERED.
enum class PartitionReason : uint32_t {
    NONE              = 0,
    HEARTBEAT_TIMEOUT = 1,
    SEQUENCE_GAP      = 2,
    DECODE_ERRORS     = 3,
    TRANSPORT_ERROR   = 4,
};
struct PartitionStateEvent {
    uint16_t partition_id;
    uint16_t reserved0;
    uint32_t reason;      // PartitionReason
    uint64_t last_seq;
    uint64_t changed_ns;
};

// TRANSPORT_DOWN / TRANSPORT_UP: the partition's transport lost or regained
// its source. Reasons are transport-neutral (INV-015); the transport-specific
// cause is in the agent log and /partitions, not here.
enum class TransportStateReason : uint32_t {
    NONE              = 0,
    SOURCE_LOST       = 1, // remote end went away (Aeron image gone, TCP peer closed)
    LOCAL_FAILURE     = 2, // agent side failed (driver timeout, socket error)
    PUBLISHER_RESTART = 3, // on UP: a different publisher instance is the source now
};
struct TransportStateEvent {
    uint16_t partition_id;
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
    GAP_WHILE_HELD = 5, // (3.4b)
    IDENTITY      = 6, // the snapshot came from another publisher instance (planned)
    SEND_FAILED   = 7, // the request could not be sent upstream
    REPLY_TOO_LARGE = 8, // the reply exceeds the partition's recap.reply_slab_bytes
};
struct RecapEvent {
    uint64_t request_id;   // 0 on PENDING (no request yet)
    uint32_t symbol_id;
    uint16_t partition_id;
    uint8_t  status;       // RecapEventStatus; 0 except on FAILED
    uint8_t  reserved0;
    uint32_t attempt;      // 1 for the first request, then +1 per retry; 0 on PENDING
    uint32_t record_count; // BEGIN: records in the reply; COMPLETE: records written to this ring
    uint64_t as_of_seq;    // S, on BEGIN and COMPLETE
};

#pragma pack(pop)

static_assert(sizeof(GapEvent) == 32);
static_assert(sizeof(PartitionStateEvent) == 24);
static_assert(sizeof(TransportStateEvent) == 24);
static_assert(sizeof(FilterReloadedEvent) == 8);
static_assert(sizeof(AgentRestartedEvent) == 16);
static_assert(sizeof(SessionClosingEvent) == 8);
static_assert(sizeof(SymbolMappedEvent) == 32);
static_assert(sizeof(RecapEvent) == 32);
static_assert(std::is_trivially_copyable_v<GapEvent>);
static_assert(std::is_trivially_copyable_v<SymbolMappedEvent>);

} // namespace subplane
