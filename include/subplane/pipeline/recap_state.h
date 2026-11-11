#pragma once

// Pipeline-side recap state of one partition: which symbols are pending, which
// sessions wait on each, and the live messages held for them until the
// snapshot reply merges (architecture.md, "Recap").
//
// Owned and touched only by the partition's pipeline thread. Every buffer is
// sized by init() on the cold path (INV-002): the pending pool and its hash
// index, the hold arena and the copy-out scratch buffer.
//
// Floor: each pending symbol has the lowest snapshot sequence S the agent can
// accept. Live messages for the symbol after the floor are held; a reply as of
// S >= floor plus the held messages above S is the whole stream for a waiter.

#include "subplane/core/message.h"
#include "subplane/recap/recap_config.h"
#include "subplane/recap/recap_manager.h"

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace subplane {

// One bit per pipeline session slot (Session::recap_slot).
struct RecapWaiters {
    static constexpr uint32_t kSlots = 256;
    uint64_t w[kSlots / 64] = {};
    void set(uint32_t i) noexcept { w[i >> 6] |= 1ull << (i & 63u); }
    void clear(uint32_t i) noexcept { w[i >> 6] &= ~(1ull << (i & 63u)); }
    bool test(uint32_t i) const noexcept { return (w[i >> 6] >> (i & 63u)) & 1u; }
    bool any() const noexcept { return (w[0] | w[1] | w[2] | w[3]) != 0; }
};

struct RecapPending {
    static constexpr uint32_t kNil = ~0u;
    bool         in_use = false;
    bool         overflowed = false; // last hold failed; RECAP_FAILED{4} already sent
    bool         parked = false;     // an OK reply with S ahead of live waits in `reply`
    uint32_t     symbol_id = 0;
    uint64_t     floor = 0;
    RecapWaiters waiters;
    // Hold list: a byte stream of (TickHeader, payload) across a chain of arena chunks.
    uint32_t     head = kNil, tail = kNil, tail_used = 0, chunks = 0;
    uint32_t     held = 0;
    RecapReply   reply;
};

class RecapState {
public:
    // Largest message the agent holds; a bigger one is treated as a hold overflow.
    static constexpr uint32_t kMaxHeldPayload = 64u << 10;

    // Cold. Sizes everything from the partition's recap config.
    void init(const RecapConfig& cfg);

    // Hot. Number of pending symbols; zero means the hot path is untouched.
    uint32_t pending() const noexcept { return n_; }
    uint32_t parked() const noexcept { return parked_; }
    uint32_t capacity() const noexcept { return static_cast<uint32_t>(pool_.size()); }

    RecapPending* find(uint32_t symbol_id) noexcept {
        if (n_ == 0) return nullptr;
        for (uint32_t i = home(symbol_id);; i = (i + 1) & mask_) {
            const uint32_t v = index_[i];
            if (v == 0) return nullptr;
            if (pool_[v - 1].symbol_id == symbol_id) return &pool_[v - 1];
        }
    }
    // nullptr when the pool is full.
    RecapPending* insert(uint32_t symbol_id, uint64_t floor) noexcept;
    // Frees the entry and its holds (and forgets a parked reply; the caller
    // releases its slab first).
    void erase(RecapPending& e) noexcept;
    RecapPending& at(uint32_t i) noexcept { return pool_[i]; }

    // INTENT: the generation rises each time the stream changes under the
    // pending recaps (gap, sequence reset, publisher restart). Every request
    // carries it and every reply echoes it; a reply from an older generation
    // is a snapshot of the stream as it was before the change and is dropped.
    uint32_t generation() const noexcept { return generation_; }
    void renew() noexcept { ++generation_; }
    // After a sequence reset or a publisher restart, until the first sequence
    // of the new stream arrives: floors are unknown, so an OK reply is parked.
    bool awaiting_seq() const noexcept { return awaiting_seq_; }
    void set_awaiting_seq(bool v) noexcept { awaiting_seq_ = v; }

    void set_parked(RecapPending& e, const RecapReply& r) noexcept {
        if (!e.parked) ++parked_;
        e.parked = true;
        e.reply = r;
    }
    void clear_parked(RecapPending& e) noexcept {
        if (e.parked) --parked_;
        e.parked = false;
    }

    // Hot. Appends one message to e's hold list; false (nothing written) when
    // the arena lacks room or the message is larger than kMaxHeldPayload.
    // `stream_seq` is the message's partition sequence: the merge compares it
    // with the snapshot's S, while h.sequence may be the instrument's.
    bool hold(RecapPending& e, const TickHeader& h, uint64_t stream_seq, const uint8_t* bytes, uint32_t len) noexcept;
    void free_holds(RecapPending& e) noexcept;

    // Calls f(const TickHeader&, const uint8_t* payload, uint32_t len) for
    // each held message in arrival order, with the stream sequence given to
    // hold() in the header's seq_tag. The payload points into a scratch
    // buffer valid until the next call.
    template <class F>
    void for_each_held(const RecapPending& e, F&& f) noexcept {
        Cursor c{e.head, 0};
        for (uint32_t i = 0; i < e.held; ++i) {
            TickHeader h;
            read(c, reinterpret_cast<uint8_t*>(&h), sizeof(h));
            read(c, scratch_.data(), h.payload_len);
            f(static_cast<const TickHeader&>(h), static_cast<const uint8_t*>(scratch_.data()), h.payload_len);
        }
    }

    uint32_t free_chunks() const noexcept { return free_count_; }
    uint32_t total_chunks() const noexcept { return static_cast<uint32_t>(next_.size()); }

private:
    struct Cursor { uint32_t chunk; uint32_t off; };

    uint32_t home(uint32_t symbol_id) const noexcept {
        return static_cast<uint32_t>((symbol_id * 0x9E3779B97F4A7C15ull) >> 32) & mask_;
    }
    void write(RecapPending& e, const uint8_t* src, uint32_t n) noexcept;
    void read(Cursor& c, uint8_t* dst, uint32_t n) const noexcept;

    std::vector<RecapPending> pool_;
    std::vector<uint32_t>     free_pool_;  // indices of unused pool entries
    std::vector<uint32_t>     index_;      // open addressing: pool index + 1, 0 = empty
    uint32_t                  mask_ = 0;
    uint32_t                  n_ = 0;
    uint32_t                  parked_ = 0;
    uint32_t                  generation_ = 0;
    bool                      awaiting_seq_ = false;

    std::vector<uint8_t>      arena_;
    std::vector<uint32_t>     next_;       // per chunk: next in its chain
    uint32_t                  chunk_bytes_ = 0;
    uint32_t                  free_head_ = RecapPending::kNil;
    uint32_t                  free_count_ = 0;
    std::vector<uint8_t>      scratch_;
};

} // namespace subplane
