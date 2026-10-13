#pragma once

// Small generic SBE encoder driven by a loaded SbeSchema. Cold-path tool code,
// but it never allocates: it writes into a caller-provided buffer.

#include <cstdint>
#include <cstring>
#include <string_view>

#include "subplane/decoder/sbe_schema.h"

namespace sptool {

// Write the low `size` bytes of v in the given byte order.
inline void put_uint(uint8_t* p, unsigned size, uint64_t v, bool little) noexcept {
    for (unsigned i = 0; i < size; ++i) {
        const unsigned shift = 8U * (little ? i : size - 1U - i);
        p[i] = static_cast<uint8_t>(v >> shift);
    }
}

// `block` points at the start of the block that owns the field (root block or
// a group entry). Returns false if the field is not an integer.
inline bool write_field(uint8_t* block, const subplane::FieldDef& f, uint64_t v, bool little) noexcept {
    using subplane::SbePrim;
    switch (f.prim) {
        case SbePrim::U8: case SbePrim::U16: case SbePrim::U32: case SbePrim::U64:
        case SbePrim::I8: case SbePrim::I16: case SbePrim::I32: case SbePrim::I64:
            put_uint(block + f.offset, f.size, v, little);
            return true;
        default:
            return false;
    }
}

inline bool write_chars(uint8_t* block, const subplane::FieldDef& f, std::string_view s) noexcept {
    if (f.prim != subplane::SbePrim::CHAR_ARRAY) return false;
    std::memset(block + f.offset, 0, f.size);
    std::memcpy(block + f.offset, s.data(), s.size() < f.size ? s.size() : f.size);
    return true;
}

// Name-based convenience: set a root-block integer field of the message that
// starts at `msg` (header included). False if the field is missing/not integer.
inline bool set_u64(const subplane::SbeSchema& s, uint8_t* msg, const subplane::MessageDef& m,
                    std::string_view name, uint64_t v) noexcept {
    const subplane::FieldDef* f = s.find_field(m, name);
    return f && write_field(msg + s.header().size, *f, v, s.little_endian());
}

// Builds one message into [buf, buf+cap): header, zeroed root block, then
// groups (dimension header + entries) and varData in the order the caller
// appends them (SBE requires root, groups, data). Overflow is sticky.
class MessageBuilder {
public:
    explicit MessageBuilder(const subplane::SbeSchema& s) : s_(s), little_(s.little_endian()) {}

    void begin(const subplane::MessageDef& m, uint8_t* buf, uint32_t cap) noexcept {
        begin_raw(m.template_id, m.block_length, buf, cap);
    }

    // Header + zeroed root block of arbitrary template id (used for unknown ids).
    void begin_raw(uint16_t template_id, uint16_t block_length, uint8_t* buf, uint32_t cap) noexcept {
        buf_ = buf; cap_ = cap; pos_ = 0; overflow_ = false; root_ = nullptr;
        const auto& h = s_.header();
        uint8_t* hdr = alloc(h.size);
        if (!hdr) return;
        put_uint(hdr + h.block_length_off, h.block_length_size, block_length, little_);
        put_uint(hdr + h.template_id_off, h.template_id_size, template_id, little_);
        put_uint(hdr + h.schema_id_off, h.schema_id_size, s_.schema_id(), little_);
        put_uint(hdr + h.version_off, h.version_size, s_.version(), little_);
        root_ = alloc(block_length);
    }

    bool ok() const noexcept { return !overflow_; }
    uint32_t size() const noexcept { return pos_; }
    uint8_t* root() const noexcept { return root_; }
    bool little() const noexcept { return little_; }

    // Writes the dimension header; entries follow via entry(). Nested groups
    // are appended right after their parent entry's root block.
    bool begin_group(const subplane::GroupDef& g, uint16_t n) noexcept {
        uint8_t* dim = alloc(g.dim.size);
        if (!dim) return false;
        put_uint(dim + g.dim.block_length_off, g.dim.block_length_size, g.block_length, little_);
        put_uint(dim + g.dim.num_in_group_off, g.dim.num_in_group_size, n, little_);
        return true;
    }
    uint8_t* entry(const subplane::GroupDef& g) noexcept { return alloc(g.block_length); }

    bool var_data(const subplane::VarDataDef& d, std::string_view v) noexcept {
        uint8_t* hdr = alloc(d.hdr_size);
        if (!hdr) return false;
        put_uint(hdr + d.len_off, d.len_size, v.size(), little_);
        uint8_t* body = alloc(static_cast<uint32_t>(v.size()));
        if (!body) return false;
        std::memcpy(body, v.data(), v.size());
        return true;
    }

private:
    uint8_t* alloc(uint32_t n) noexcept {
        if (overflow_ || n > cap_ - pos_) { overflow_ = true; return nullptr; }
        uint8_t* p = buf_ + pos_;
        std::memset(p, 0, n);
        pos_ += n;
        return p;
    }

    const subplane::SbeSchema& s_;
    bool little_;
    uint8_t* buf_ = nullptr;
    uint32_t cap_ = 0, pos_ = 0;
    bool overflow_ = false;
    uint8_t* root_ = nullptr;
};

} // namespace sptool
