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

#pragma pack(pop)

static_assert(sizeof(GapEvent) == 32);
static_assert(sizeof(PartitionStateEvent) == 24);
static_assert(sizeof(FilterReloadedEvent) == 8);
static_assert(sizeof(AgentRestartedEvent) == 16);
static_assert(sizeof(SessionClosingEvent) == 8);
static_assert(sizeof(SymbolMappedEvent) == 32);
static_assert(std::is_trivially_copyable_v<GapEvent>);
static_assert(std::is_trivially_copyable_v<SymbolMappedEvent>);

} // namespace subplane
