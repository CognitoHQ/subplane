#include "subplane/pipeline/recap_state.h"

namespace subplane {

void RecapState::init(const RecapConfig& cfg) {
    pool_.assign(cfg.max_pending, RecapPending{});
    free_pool_.clear();
    free_pool_.reserve(cfg.max_pending);
    for (uint32_t i = cfg.max_pending; i > 0; --i) free_pool_.push_back(i - 1);
    uint32_t cap = 16;
    while (cap < 2u * cfg.max_pending) cap <<= 1;
    index_.assign(cap, 0);
    mask_ = cap - 1;
    n_ = 0;
    parked_ = 0;
    generation_ = 0;
    awaiting_seq_ = false;

    chunk_bytes_ = cfg.hold_chunk_bytes;
    const uint32_t chunks = static_cast<uint32_t>(cfg.hold_bytes / cfg.hold_chunk_bytes);
    arena_.assign(static_cast<size_t>(chunks) * chunk_bytes_, 0);
    next_.resize(chunks);
    for (uint32_t i = 0; i < chunks; ++i) next_[i] = i + 1 < chunks ? i + 1 : RecapPending::kNil;
    free_head_ = chunks ? 0 : RecapPending::kNil;
    free_count_ = chunks;
    scratch_.assign(kMaxHeldPayload, 0);
}

RecapPending* RecapState::insert(uint32_t symbol_id, uint64_t floor) noexcept {
    if (free_pool_.empty()) return nullptr;
    const uint32_t idx = free_pool_.back();
    free_pool_.pop_back();
    RecapPending& e = pool_[idx];
    e = RecapPending{};
    e.in_use = true;
    e.symbol_id = symbol_id;
    e.floor = floor;
    uint32_t i = home(symbol_id);
    while (index_[i] != 0) i = (i + 1) & mask_;
    index_[i] = idx + 1;
    ++n_;
    return &e;
}

void RecapState::erase(RecapPending& e) noexcept {
    const uint32_t idx = static_cast<uint32_t>(&e - pool_.data());
    free_holds(e);
    clear_parked(e);
    uint32_t i = home(e.symbol_id);
    while (index_[i] != idx + 1) i = (i + 1) & mask_;
    // INTENT: backward-shift deletion keeps linear probing correct without
    // tombstones: every later entry of the probe run whose home is not
    // between the hole and itself moves into the hole. Leaving a plain hole
    // would hide those entries from find().
    index_[i] = 0;
    for (uint32_t j = (i + 1) & mask_; index_[j] != 0; j = (j + 1) & mask_) {
        const uint32_t k = home(pool_[index_[j] - 1].symbol_id);
        const bool stays = i <= j ? (i < k && k <= j) : (i < k || k <= j);
        if (stays) continue;
        index_[i] = index_[j];
        index_[j] = 0;
        i = j;
    }
    e = RecapPending{};
    free_pool_.push_back(idx);
    --n_;
}

bool RecapState::hold(RecapPending& e, const TickHeader& h, const uint8_t* bytes, uint32_t len) noexcept {
    if (len > kMaxHeldPayload) return false;
    const uint64_t need = sizeof(TickHeader) + static_cast<uint64_t>(len);
    const uint64_t room = (e.tail == RecapPending::kNil ? 0 : chunk_bytes_ - e.tail_used) +
                          static_cast<uint64_t>(free_count_) * chunk_bytes_;
    if (need > room) return false;
    TickHeader hh = h;
    hh.payload_len = len;
    write(e, reinterpret_cast<const uint8_t*>(&hh), sizeof(hh));
    write(e, bytes, len);
    ++e.held;
    return true;
}

void RecapState::free_holds(RecapPending& e) noexcept {
    if (e.head != RecapPending::kNil) {
        // O(1): the whole chain goes to the front of the free list.
        next_[e.tail] = free_head_;
        free_head_ = e.head;
        free_count_ += e.chunks;
    }
    e.head = e.tail = RecapPending::kNil;
    e.tail_used = 0;
    e.chunks = 0;
    e.held = 0;
}

void RecapState::write(RecapPending& e, const uint8_t* src, uint32_t n) noexcept {
    while (n != 0) {
        if (e.tail == RecapPending::kNil || e.tail_used == chunk_bytes_) {
            const uint32_t c = free_head_; // hold() checked the room
            free_head_ = next_[c];
            --free_count_;
            next_[c] = RecapPending::kNil;
            if (e.tail == RecapPending::kNil) e.head = c;
            else next_[e.tail] = c;
            e.tail = c;
            e.tail_used = 0;
            ++e.chunks;
        }
        const uint32_t k = n < chunk_bytes_ - e.tail_used ? n : chunk_bytes_ - e.tail_used;
        std::memcpy(arena_.data() + static_cast<size_t>(e.tail) * chunk_bytes_ + e.tail_used, src, k);
        e.tail_used += k;
        src += k;
        n -= k;
    }
}

void RecapState::read(Cursor& c, uint8_t* dst, uint32_t n) const noexcept {
    while (n != 0) {
        if (c.off == chunk_bytes_) {
            c.chunk = next_[c.chunk];
            c.off = 0;
        }
        const uint32_t k = n < chunk_bytes_ - c.off ? n : chunk_bytes_ - c.off;
        std::memcpy(dst, arena_.data() + static_cast<size_t>(c.chunk) * chunk_bytes_ + c.off, k);
        c.off += k;
        dst += k;
        n -= k;
    }
}

} // namespace subplane
