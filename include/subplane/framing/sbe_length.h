#pragma once

// SBE message length from the schema alone (no length prefix required).
//
// At schema load each message template is compiled into a small "walk
// program": one op per group begin/end and per varData. walk() then computes
// the total message length with bounds checks, a fixed-depth stack, no
// allocation, no XML and no virtual calls.

#include <bit>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "subplane/decoder/sbe_schema.h"

namespace subplane {

inline constexpr unsigned SBE_MAX_GROUP_DEPTH = 4;

struct WalkOp {
    enum Kind : uint8_t { ROOT, GROUP_BEGIN, GROUP_END, VAR_DATA };
    uint8_t  kind          = ROOT;
    uint8_t  dim_block_len_size = 0; // GROUP_BEGIN: size of blockLength member
    uint8_t  dim_num_size       = 0; // GROUP_BEGIN: size of numInGroup member
    uint8_t  len_prefix_size    = 0; // VAR_DATA: size of the length member
    uint16_t dim_block_len_offset = 0; // GROUP_BEGIN: blockLength offset in dimension header
    uint16_t dim_num_offset       = 0; // GROUP_BEGIN: numInGroup offset; VAR_DATA: length member offset
    uint16_t hdr_size      = 0;      // GROUP_BEGIN: dimension header bytes; VAR_DATA: bytes before data
    uint16_t jump          = 0;      // GROUP_BEGIN: index of matching GROUP_END; GROUP_END: index of GROUP_BEGIN
    uint16_t since_version = 0;      // GROUP_BEGIN / VAR_DATA: skipped when the message's acting version is older
};

struct WalkProgram {
    const WalkOp* ops   = nullptr; // group/data ops after the root block (ROOT is implicit)
    uint16_t      count = 0;
};

enum class WalkStatus : uint8_t { OK, UNKNOWN_TEMPLATE, TRUNCATED, MALFORMED };

namespace detail {
// Read an unsigned integer of 1/2/4/8 bytes with the schema's byte order.
inline uint64_t sbe_read_uint(const uint8_t* p, unsigned size, bool little) noexcept {
    uint64_t v = 0;
    switch (size) {
        case 1: return p[0];
        case 2: { uint16_t x; std::memcpy(&x, p, 2); v = (little == (std::endian::native == std::endian::little)) ? x : __builtin_bswap16(x); break; }
        case 4: { uint32_t x; std::memcpy(&x, p, 4); v = (little == (std::endian::native == std::endian::little)) ? x : __builtin_bswap32(x); break; }
        case 8: { uint64_t x; std::memcpy(&x, p, 8); v = (little == (std::endian::native == std::endian::little)) ? x : __builtin_bswap64(x); break; }
        default: break;
    }
    return v;
}
} // namespace detail

class SbeLengthWalker {
public:
    // Cold. Fails if group nesting exceeds SBE_MAX_GROUP_DEPTH.
    bool build(const SbeSchema& schema, std::string& err);

    // Hot. nullptr if the template is not in the schema.
    const WalkProgram* program(uint16_t template_id) const noexcept {
        return template_id < programs_.size() && programs_[template_id].valid ? &programs_[template_id].prog : nullptr;
    }

    uint32_t header_size() const noexcept { return header_.size; }
    const HeaderLayout& header() const noexcept { return header_; }
    bool little_endian() const noexcept { return little_; }

    // Hot. Total bytes of the message starting at msg (header included).
    WalkStatus walk(const uint8_t* msg, uint32_t avail, uint32_t& total_len) const noexcept;

    // Hot. Continue from `pos` = end of the root block (caller already read the
    // header). `acting_version` is the version field of that header: groups and
    // varData with a larger sinceVersion are not on the wire and are skipped.
    WalkStatus walk_program(const WalkProgram& prog, const uint8_t* msg, uint32_t avail,
                            uint32_t pos, uint16_t acting_version, uint32_t& total_len) const noexcept;

private:
    struct Entry { WalkProgram prog; bool valid = false; };
    HeaderLayout       header_;
    bool               little_ = true;
    std::vector<WalkOp> ops_;
    std::vector<Entry>  programs_;
};

} // namespace subplane
