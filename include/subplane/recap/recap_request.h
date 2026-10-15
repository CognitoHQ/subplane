#pragma once

// The recap request the agent sends upstream to a publisher's snapshot service
// (`<service>-snap`). subplane's own format: 64 bytes, little-endian,
// independent of the bus schema. The reply comes back in the stream's own
// schema between RECAP_BEGIN and RECAP_END markers that echo request_id.
//
// INTENT: part of the publisher-facing contract. A snapshot service written
// against version 1 must keep working: add fields only in the reserved bytes
// and bump kRecapRequestVersion when the meaning of existing bytes changes.

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace subplane {

inline constexpr uint32_t kRecapRequestMagic = 0x51525053u; // "SPRQ" read as little-endian bytes
inline constexpr uint16_t kRecapRequestVersion = 1;

struct RecapRequestWire {
    uint32_t magic = kRecapRequestMagic;
    uint16_t version = kRecapRequestVersion;
    uint16_t flags = 0;
    uint64_t request_id = 0;        // unique per agent epoch; echoed in the reply markers
    uint64_t agent_id = 0;          // the agent's epoch
    uint64_t instrument = 0;        // the symbol as on the wire (not the agent's symbol_id)
    uint16_t venue = 0;             // the venue as on the wire, 0 if unmapped
    uint8_t  reserved0[6] = {};
    uint64_t last_seq_seen = 0;     // last sequence the agent saw on the partition, 0 if none
    uint64_t source_epoch_seen = 0; // publisher identity the agent sees, 0 if unknown
    uint64_t reserved1 = 0;
};

static_assert(sizeof(RecapRequestWire) == 64);
static_assert(std::is_trivially_copyable_v<RecapRequestWire>);
static_assert(offsetof(RecapRequestWire, version) == 4);
static_assert(offsetof(RecapRequestWire, request_id) == 8);
static_assert(offsetof(RecapRequestWire, agent_id) == 16);
static_assert(offsetof(RecapRequestWire, instrument) == 24);
static_assert(offsetof(RecapRequestWire, venue) == 32);
static_assert(offsetof(RecapRequestWire, last_seq_seen) == 40);
static_assert(offsetof(RecapRequestWire, source_epoch_seen) == 48);
static_assert(offsetof(RecapRequestWire, reserved1) == 56);

} // namespace subplane
