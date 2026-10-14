#pragma once

#include "subplane/core/message.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace subplane {

// Fixed-capacity, append-only, lock-free interning table.
// Ids start at 1; 0 means unknown / table full / invalid key.
//
// INTENT: no mutex. A hash cell goes 0 -> CLAIMED (CAS) -> id (release). The
// winner of the CAS owns the entry until it publishes the id, so concurrent
// inserters of the same key converge on one id (losers spin briefly on
// CLAIMED, then compare). Readers never block: a CLAIMED cell is treated as a
// miss (the insert has not completed). Cells are never removed or moved, so a
// reader can never miss a completed insert because of a rehash. Replacing this
// with a growing table would reintroduce reader/writer locking.
// Strings longer than 24 bytes (or empty) are rejected (return 0).
class SymbolTable {
public:
    static constexpr std::size_t kMaxName = 24;

    explicit SymbolTable(uint32_t max_symbols);

    // Hot-capable. Returns existing or new id; 0 if full or name invalid.
    uint32_t intern(std::string_view name) noexcept;
    uint32_t lookup(std::string_view name) const noexcept;

    // Integer instrument ids (dense mapping into the same id space).
    // `venue` != 0 scopes the id to a venue (decoder `symbol_scope: venue`):
    // (10, 1) and (20, 1) are different symbols; venue 0 is the global scope.
    uint32_t intern_u64(uint64_t key, uint16_t venue = 0) noexcept;
    uint32_t lookup_u64(uint64_t key, uint16_t venue = 0) const noexcept;
    uint64_t key_u64(uint32_t id) const; // 0 if id is not a u64 entry
    uint16_t venue_u64(uint32_t id) const; // 0 if global scope or not a u64 entry

    // Cold. false if id unknown. u64 entries resolve to their decimal string,
    // venue-scoped ones to "<venue>:<id>".
    bool resolve(uint32_t id, std::string& out) const;

    uint32_t size() const noexcept { return count_.load(std::memory_order_acquire); }
    uint32_t capacity() const noexcept { return max_; }

private:
    struct Entry {
        uint64_t key = 0;     // u64 key, or hash for strings
        char name[kMaxName] = {};
        uint8_t len = 0;
        uint8_t is_u64 = 0;
        uint16_t venue = 0;   // u64 entries only; 0 = global scope
        std::atomic<uint8_t> ready{0};
    };
    static constexpr uint32_t kEmpty = 0, kClaimed = 0xFFFFFFFFu, kDead = 0xFFFFFFFEu;

    uint32_t find(uint64_t hash, std::string_view name, bool is_u64, uint64_t key, uint16_t venue) const noexcept;
    uint32_t insert(uint64_t hash, std::string_view name, bool is_u64, uint64_t key, uint16_t venue) noexcept;
    bool matches(uint32_t id, std::string_view name, bool is_u64, uint64_t key, uint16_t venue) const noexcept;

    uint32_t max_;
    uint32_t mask_;
    std::atomic<uint32_t> count_{0};   // ids handed out (<= max_)
    std::atomic<uint32_t> next_{1};
    std::unique_ptr<std::atomic<uint32_t>[]> cells_;
    std::unique_ptr<Entry[]> entries_; // indexed by id
};

// Venue ids (uint16_t) on top of SymbolTable.
class VenueTable {
public:
    explicit VenueTable(uint16_t max_venues = 256) : t_(max_venues) {}
    VenueId intern(std::string_view name) noexcept { return static_cast<VenueId>(t_.intern(name)); }
    VenueId lookup(std::string_view name) const noexcept { return static_cast<VenueId>(t_.lookup(name)); }
    bool resolve(VenueId id, std::string& out) const { return t_.resolve(id, out); }
    uint32_t size() const noexcept { return t_.size(); }
    uint32_t capacity() const noexcept { return t_.capacity(); }

private:
    SymbolTable t_;
};

// Cold. Parses a symbol spelling from config, the control channel or HTTP:
// "<id>" (numeric, venue 0) or "<venue>:<id>" (both numeric, venue 1..65535).
// Returns false for anything else, which callers treat as a name.
bool parse_numeric_symbol(std::string_view s, uint64_t& key, uint16_t& venue) noexcept;

} // namespace subplane
