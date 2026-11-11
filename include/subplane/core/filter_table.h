#pragma once

#include "subplane/core/message.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace subplane {

// Per-session filter. Built on the cold path, immutable afterwards, swapped
// atomically (by pointer) by the owner. accept() is a handful of AND/shift ops.
struct FilterTable {
    std::vector<uint64_t> symbol_bits; // sized (max_symbols+63)/64 at build, never resized
    uint64_t msg_type_mask  = 0;       // bit i => MsgType i (i < 64) accepted
    // bit i => topic_id i accepted; 512 words cover every topic_id (1..32767).
    // INTENT: a bitmap by topic_id, not by an agent-local slot, so a record read
    // back from the LVC (any partition, cold) is filtered the same as live data.
    uint64_t topic_bits[512] = {};
    uint64_t venue_mask     = ~0ull;   // bit i => venue i accepted; ids >= 64 accepted iff mask == ~0ull
    bool all_symbols   = false;
    bool bus_admin     = true;         // BUS_ADMIN is topic-scoped
    bool agent_control = true;         // passes whatever the topic
    uint32_t generation = 0;

    // Hot. No allocation, no string compare.
    // INTENT: every shift is guarded by a range check (ids >= 64 would be UB on
    // a 64-bit shift) and symbol_id is bounds-checked against the bitset: ids
    // come off the wire/decoder, so an out-of-range id must reject, not read
    // past the vector. Removing either is an out-of-bounds read / UB.
    bool accept(const TickHeader& h) const noexcept {
        switch (static_cast<Kind>(h.kind)) {
        case Kind::AGENT_CONTROL: return agent_control;
        case Kind::BUS_ADMIN:     return bus_admin && topic_ok(h.topic_id);
        case Kind::DATA: break;
        default: return false; // PAD and unknown kinds
        }
        if (!topic_ok(h.topic_id)) return false;
        if (h.msg_type >= 64 || !((msg_type_mask >> h.msg_type) & 1u)) return false;
        if (venue_mask != ~0ull && (h.venue_id >= 64 || !((venue_mask >> h.venue_id) & 1u))) return false;
        if (all_symbols) return true;
        const uint32_t w = h.symbol_id >> 6;
        if (w >= symbol_bits.size()) return false;
        return (symbol_bits[w] >> (h.symbol_id & 63u)) & 1u;
    }

private:
    bool topic_ok(uint16_t t) const noexcept { return t < 32768 && ((topic_bits[t >> 6] >> (t & 63u)) & 1u); }
};

// Cold description used to build a FilterTable.
struct FilterSpec {
    std::vector<uint32_t> symbol_ids;
    bool all_symbols = false;
    std::vector<uint8_t> msg_types;
    std::vector<uint16_t> topic_ids;
    std::vector<uint16_t> venue_ids; // empty = all
    bool bus_admin = true;
    bool agent_control = true;
};

// Cold. Returns nullptr and sets err if any id is out of range.
std::unique_ptr<FilterTable> build_filter(const FilterSpec& spec, uint32_t max_symbols,
                                          uint32_t generation, std::string& err);

} // namespace subplane
