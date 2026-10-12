#include "flashtick/pipeline/lvc.h"

#include <cstring>

namespace flashtick {

namespace {
constexpr size_t kVerBytes = 64;
constexpr size_t kHdrBytes = 64;
} // namespace

bool Lvc::init(uint32_t max_symbols, const std::vector<uint8_t>& msg_types, uint32_t max_payload, std::string& err) {
    std::memset(type_index_, 0xFF, sizeof(type_index_));
    types_.clear();
    for (uint8_t t : msg_types) {
        if (t >= 128) {
            err = "lvc: msg_type code out of range";
            return false;
        }
        if (type_index_[t] == 0xFF) {
            type_index_[t] = static_cast<uint8_t>(types_.size());
            types_.push_back(t);
        }
    }
    max_symbols_ = max_symbols;
    max_payload_ = max_payload;
    n_types_ = static_cast<uint32_t>(types_.size());
    stride_ = kVerBytes + kHdrBytes + ((static_cast<size_t>(max_payload) + 63) & ~static_cast<size_t>(63));
    const size_t total = static_cast<size_t>(max_symbols_) * n_types_ * stride_ + 64;
    if (n_types_ == 0 || max_symbols_ == 0) {
        storage_.reset();
        base_ = nullptr;
        return true; // nothing tracked; update() is a no-op
    }
    storage_.reset(new (std::nothrow) uint8_t[total]());
    if (!storage_) {
        err = "lvc: allocation of " + std::to_string(total) + " bytes failed";
        return false;
    }
    // Align base to 64.
    auto raw = reinterpret_cast<uintptr_t>(storage_.get());
    base_ = reinterpret_cast<uint8_t*>((raw + 63) & ~static_cast<uintptr_t>(63));
    for (size_t i = 0; i < static_cast<size_t>(max_symbols_) * n_types_; ++i) {
        new (base_ + i * stride_) Slot();
    }
    return true;
}

void Lvc::update(const TickHeader& h, const uint8_t* bytes, uint32_t len) noexcept {
    if (base_ == nullptr || h.msg_type >= 128 || h.symbol_id >= max_symbols_) return;
    const uint8_t ti = type_index_[h.msg_type];
    if (ti == 0xFF) return;
    uint8_t* s = slot(h.symbol_id, ti);
    auto* ver = reinterpret_cast<std::atomic<uint64_t>*>(s);

    // INTENT: seqlock write. Odd version = write in progress. The release
    // fence orders the version bump before the payload stores; the final
    // release store publishes. Readers retry on odd/changed version. Payload
    // is copied with memcpy for speed (same trade-off as the ring).
    const uint64_t v = ver->load(std::memory_order_relaxed);
    ver->store(v + 1, std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_release);

    TickHeader copy = h;
    copy.seq_tag = 0;
    uint32_t copy_len = len;
    if (len > max_payload_) {
        copy_len = 0;
        copy.flags = static_cast<uint8_t>(copy.flags | FLAG_LVC_TRUNCATED);
    }
    copy.payload_len = copy_len;
    std::memcpy(s + kVerBytes, &copy, sizeof(TickHeader));
    if (copy_len) std::memcpy(s + kVerBytes + kHdrBytes, bytes, copy_len);

    ver->store(v + 2, std::memory_order_release);
    writes_.store(writes_.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
}

bool Lvc::read(uint32_t symbol_id, uint8_t msg_type, TickHeader& hdr, std::vector<uint8_t>& payload) const {
    if (base_ == nullptr || msg_type >= 128 || symbol_id >= max_symbols_) return false;
    const uint8_t ti = type_index_[msg_type];
    if (ti == 0xFF) return false;
    const uint8_t* s = slot(symbol_id, ti);
    const auto* ver = reinterpret_cast<const std::atomic<uint64_t>*>(s);
    for (int spin = 0; spin < 1000; ++spin) {
        const uint64_t v1 = ver->load(std::memory_order_acquire);
        if (v1 == 0) return false;   // never written
        if (v1 & 1u) continue;
        TickHeader h;
        std::memcpy(&h, s + kVerBytes, sizeof(TickHeader));
        const uint32_t len = h.payload_len <= max_payload_ ? h.payload_len : 0;
        payload.resize(len);
        if (len) std::memcpy(payload.data(), s + kVerBytes + kHdrBytes, len);
        std::atomic_thread_fence(std::memory_order_acquire);
        if (ver->load(std::memory_order_relaxed) == v1) {
            hdr = h;
            return true;
        }
    }
    return false;
}

} // namespace flashtick
