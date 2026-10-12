#pragma once

// Last-value cache: latest record per (symbol_id, tracked msg_type).
// Single writer (the pipeline thread), many cold readers (control channel,
// HTTP). Each slot is a seqlock: version, TickHeader, payload bytes.

#include "flashtick/core/message.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace flashtick {

class Lvc {
public:
    // Cold. `msg_types` are the data msg_type codes to track (others are ignored).
    bool init(uint32_t max_symbols, const std::vector<uint8_t>& msg_types, uint32_t max_payload, std::string& err);

    // Hot. Ignores untracked msg types and symbol ids >= max_symbols.
    // Payloads larger than max_payload store the header only with FLAG_LVC_TRUNCATED.
    void update(const TickHeader& h, const uint8_t* bytes, uint32_t len) noexcept;

    // Cold. Returns false if untracked, out of range, or never written.
    bool read(uint32_t symbol_id, uint8_t msg_type, TickHeader& hdr, std::vector<uint8_t>& payload) const;

    uint32_t max_symbols() const noexcept { return max_symbols_; }
    uint32_t max_payload() const noexcept { return max_payload_; }
    const std::vector<uint8_t>& tracked_types() const noexcept { return types_; }
    bool tracks(uint8_t msg_type) const noexcept { return msg_type < 128 && type_index_[msg_type] != 0xFF; }
    uint64_t writes() const noexcept { return writes_.load(std::memory_order_relaxed); }

private:
    struct Slot {
        alignas(64) std::atomic<uint64_t> ver{0};
        // followed by TickHeader at +64 and payload at +128 within the stride
    };
    uint8_t* slot(uint32_t symbol_id, uint32_t type_idx) noexcept {
        return base_ + (static_cast<size_t>(symbol_id) * n_types_ + type_idx) * stride_;
    }
    const uint8_t* slot(uint32_t symbol_id, uint32_t type_idx) const noexcept {
        return base_ + (static_cast<size_t>(symbol_id) * n_types_ + type_idx) * stride_;
    }

    uint32_t max_symbols_ = 0;
    uint32_t max_payload_ = 0;
    uint32_t n_types_ = 0;
    size_t   stride_ = 0;
    uint8_t  type_index_[128];
    std::vector<uint8_t> types_;
    std::unique_ptr<uint8_t[]> storage_;
    uint8_t* base_ = nullptr;
    std::atomic<uint64_t> writes_{0};
};

} // namespace flashtick
