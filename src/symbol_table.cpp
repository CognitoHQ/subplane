#include "subplane/symbol_table.h"

#include <bit>
#include <cstring>

namespace subplane {

namespace {
uint64_t hash_bytes(std::string_view s) noexcept { // FNV-1a
    uint64_t h = 1469598103934665603ull;
    for (char c : s) h = (h ^ static_cast<unsigned char>(c)) * 1099511628211ull;
    return h;
}
uint64_t hash_u64(uint64_t x) noexcept { // splitmix64 finalizer
    x += 0x9E3779B97F4A7C15ull;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
    return x ^ (x >> 31);
}
} // namespace

SymbolTable::SymbolTable(uint32_t max_symbols) : max_(max_symbols) {
    // 4x cells keeps load <= 25% and leaves slack for cells burned by racing
    // inserters when the table fills.
    const uint32_t cells = std::bit_ceil(max_symbols < 4 ? 16u : max_symbols * 4u);
    mask_ = cells - 1;
    cells_ = std::make_unique<std::atomic<uint32_t>[]>(cells);
    entries_ = std::make_unique<Entry[]>(static_cast<std::size_t>(max_symbols) + 1);
}

bool SymbolTable::matches(uint32_t id, std::string_view name, bool is_u64, uint64_t key, uint16_t venue) const noexcept {
    const Entry& e = entries_[id];
    if ((e.is_u64 != 0) != is_u64) return false;
    if (is_u64) return e.key == key && e.venue == venue;
    return e.len == name.size() && std::memcmp(e.name, name.data(), name.size()) == 0;
}

uint32_t SymbolTable::find(uint64_t hash, std::string_view name, bool is_u64, uint64_t key, uint16_t venue) const noexcept {
    uint32_t i = static_cast<uint32_t>(hash) & mask_;
    for (uint32_t n = 0; n <= mask_; ++n, i = (i + 1) & mask_) {
        const uint32_t c = cells_[i].load(std::memory_order_acquire);
        if (c == kEmpty) return 0;
        if (c == kClaimed || c == kDead) continue;
        if (matches(c, name, is_u64, key, venue)) return c;
    }
    return 0;
}

uint32_t SymbolTable::insert(uint64_t hash, std::string_view name, bool is_u64, uint64_t key, uint16_t venue) noexcept {
    uint32_t i = static_cast<uint32_t>(hash) & mask_;
    for (uint32_t n = 0; n <= mask_; ++n, i = (i + 1) & mask_) {
        uint32_t c = cells_[i].load(std::memory_order_acquire);
        for (;;) {
            if (c == kEmpty) {
                if (next_.load(std::memory_order_relaxed) > max_) return 0; // full
                if (!cells_[i].compare_exchange_strong(c, kClaimed, std::memory_order_acq_rel,
                                                       std::memory_order_acquire))
                    continue; // c reloaded by the failed CAS
                const uint32_t id = next_.fetch_add(1, std::memory_order_relaxed);
                if (id > max_) { // lost the race for the last id
                    cells_[i].store(kDead, std::memory_order_release);
                    return 0;
                }
                Entry& e = entries_[id];
                e.is_u64 = is_u64 ? 1 : 0;
                if (is_u64) {
                    e.key = key;
                    e.venue = venue;
                } else {
                    e.key = hash;
                    e.len = static_cast<uint8_t>(name.size());
                    std::memcpy(e.name, name.data(), name.size());
                }
                e.ready.store(1, std::memory_order_release);
                cells_[i].store(id, std::memory_order_release);
                count_.fetch_add(1, std::memory_order_release);
                return id;
            }
            if (c == kClaimed) { // another inserter is publishing this cell; wait for it
                c = cells_[i].load(std::memory_order_acquire);
                continue;
            }
            break;
        }
        if (c != kDead && matches(c, name, is_u64, key, venue)) return c;
    }
    return 0;
}

uint32_t SymbolTable::intern(std::string_view name) noexcept {
    if (name.empty() || name.size() > kMaxName) return 0;
    const uint64_t h = hash_bytes(name);
    if (uint32_t id = find(h, name, false, 0, 0)) return id;
    return insert(h, name, false, 0, 0);
}

uint32_t SymbolTable::lookup(std::string_view name) const noexcept {
    if (name.empty() || name.size() > kMaxName) return 0;
    return find(hash_bytes(name), name, false, 0, 0);
}

uint32_t SymbolTable::intern_u64(uint64_t key, uint16_t venue) noexcept {
    const uint64_t h = hash_u64(key ^ (static_cast<uint64_t>(venue) << 48));
    if (uint32_t id = find(h, {}, true, key, venue)) return id;
    return insert(h, {}, true, key, venue);
}

uint32_t SymbolTable::lookup_u64(uint64_t key, uint16_t venue) const noexcept {
    return find(hash_u64(key ^ (static_cast<uint64_t>(venue) << 48)), {}, true, key, venue);
}

uint64_t SymbolTable::key_u64(uint32_t id) const {
    if (id == 0 || id > max_ || !entries_[id].ready.load(std::memory_order_acquire)) return 0;
    return entries_[id].is_u64 ? entries_[id].key : 0;
}

uint16_t SymbolTable::venue_u64(uint32_t id) const {
    if (id == 0 || id > max_ || !entries_[id].ready.load(std::memory_order_acquire)) return 0;
    return entries_[id].is_u64 ? entries_[id].venue : 0;
}

bool SymbolTable::resolve(uint32_t id, std::string& out) const {
    if (id == 0 || id > max_ || !entries_[id].ready.load(std::memory_order_acquire)) return false;
    const Entry& e = entries_[id];
    if (e.is_u64) out = e.venue ? std::to_string(e.venue) + ":" + std::to_string(e.key) : std::to_string(e.key);
    else out.assign(e.name, e.len);
    return true;
}

bool parse_numeric_symbol(std::string_view s, uint64_t& key, uint16_t& venue) noexcept {
    auto digits = [](std::string_view d, uint64_t& v) {
        if (d.empty() || d.size() > 19) return false;
        v = 0;
        for (char c : d) {
            if (c < '0' || c > '9') return false;
            v = v * 10 + static_cast<uint64_t>(c - '0');
        }
        return true;
    };
    const size_t colon = s.find(':');
    if (colon == std::string_view::npos) {
        venue = 0;
        return digits(s, key);
    }
    uint64_t v = 0;
    if (!digits(s.substr(0, colon), v) || v == 0 || v > 0xFFFF) return false;
    if (!digits(s.substr(colon + 1), key)) return false;
    venue = static_cast<uint16_t>(v);
    return true;
}

} // namespace subplane
