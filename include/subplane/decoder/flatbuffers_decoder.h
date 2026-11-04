#pragma once

// FlatBuffers routing decoder. Routing fields are located by field id through
// the vtable (flatbuffers_wire.h), so no schema and no FlatBuffers library are
// needed: YAML names the ids. The delivered unit is the publisher's complete
// buffer, size prefix included when the bus uses one (a sub-table is not
// decodable on its own and stripping the prefix would misalign 8-byte scalars).
//
// FlatBuffers has no message header, so the message type comes from either the
// 4-byte file identifier after the root offset (`dispatch: identifier`) or a
// scalar in the root table, typically a union's type field
// (`dispatch: {type_field: {path: [2], type: u8}}`).
//
//   decoder:
//     type: flatbuffers
//     size_prefixed: true                 # default true: buffer starts with u32 size
//     symbol_id_mode: passthrough         # passthrough (integer symbol) | table (integer or string)
//     dispatch: { type_field: { path: [2], type: u8 } }    # or: dispatch: identifier
//     messages:
//       - { type: 1, msg_type: TRADE,                      # identifier: "TRDE" with identifier dispatch
//           symbol: { path: [3, 0, 0], type: u32 },        # ids root-first; all but the last are table fields
//           venue:  { path: [3, 0, 1], type: u16 },
//           exchange_ts: { path: [1], type: u64 },
//           sequence:    { path: [0], type: u64 } }
//       - { type: 3, admin: HEARTBEAT }
//       - { type: 21, admin: RECAP_BEGIN,                  # recap reply markers (see architecture.md, Recap)
//           fields: { request_id: { path: [4], type: u64 }, as_of: { path: [5], type: u64 } } }

#include <cstdint>
#include <inplace_vector>
#include <string>
#include <vector>

#include <yaml-cpp/yaml.h>

#include "subplane/core/message.h"
#include "subplane/decoder/decoder_common.h"

namespace subplane {

enum class FbScalar : uint8_t { NONE = 0, U8, U16, U32, U64, I8, I16, I32, I64, STRING };
bool parse_fb_scalar(std::string_view name, FbScalar& out);

struct FbFieldPath {
    static constexpr size_t kMaxDepth = 4;
    std::inplace_vector<uint16_t, kMaxDepth> ids;   // empty = not mapped
    FbScalar type = FbScalar::NONE;
    bool mapped() const noexcept { return !ids.empty(); }
};

struct FbMessageSpec {
    uint32_t    type = 0;               // type_field dispatch
    char        identifier[4] = {};     // identifier dispatch
    bool        is_admin = false;
    uint8_t     msg_type = 0;           // MsgType or ControlCode
    RecapMarker marker = RecapMarker::NONE;   // admin: RECAP_BEGIN | RECAP_END | RECAP_REJECT
    FbFieldPath symbol, venue, exchange_ts, sequence;
    FbFieldPath recap[kRecapFields];          // `fields:` (markers only), integer paths
};

enum class FbDispatch : uint8_t { TYPE_FIELD, IDENTIFIER };

struct FlatbuffersDecoderConfig {
    bool                       size_prefixed = true;
    SymbolIdMode               symbol_mode = SymbolIdMode::PASSTHROUGH;
    bool                       venue_scope = false;   // symbol_scope: venue
    FbDispatch                 dispatch = FbDispatch::TYPE_FIELD;
    FbFieldPath                type_field;     // TYPE_FIELD dispatch
    std::vector<FbMessageSpec> messages;
};

bool parse_flatbuffers_decoder_config(const YAML::Node& node, FlatbuffersDecoderConfig& out, std::string& err);

class FlatbuffersDecoder {
public:
    static constexpr uint32_t kMaxMessages = 256;

    // Cold.
    bool init(const FlatbuffersDecoderConfig& cfg, std::string& err);

    // Hot. `avail` must be the whole buffer (prefix included when size_prefixed).
    DecodeResult decode(const uint8_t* msg, uint32_t avail, TickHeader& out, SymbolResolver& syms) const noexcept;

    // Cold side of the recap merge (not the live hot path). Reads a marker's
    // mapped fields. False when the buffer is not a mapped recap marker.
    bool read_recap_fields(const uint8_t* msg, uint32_t avail, RecapMarkerFields& out) const noexcept;

    const std::vector<std::string>& warnings() const { return warnings_; }
    // Cold. Whether a message maps this marker; whether any symbol is a string.
    bool maps_marker(RecapMarker m) const noexcept {
        for (const auto& e : entries_) if (e.marker == m) return true;
        return false;
    }
    bool has_string_symbol() const noexcept {
        for (const auto& e : entries_) if (e.symbol.mapped() && e.symbol.type == FbScalar::STRING) return true;
        return false;
    }
    bool symbol_passthrough() const noexcept { return passthrough_; }
    bool symbol_venue_scope() const noexcept { return venue_scope_; }
    bool size_prefixed() const noexcept { return prefix_ == 4; }

private:
    struct Entry {
        bool        present = false;
        bool        admin = false;
        uint8_t     msg_type = 0;
        uint16_t    template_id = 0;
        char        identifier[4] = {};
        RecapMarker marker = RecapMarker::NONE;
        FbFieldPath symbol, venue, exchange_ts, sequence;
        FbFieldPath recap[kRecapFields];
    };
    const Entry* find(const uint8_t* msg, uint32_t avail, uint32_t root, uint16_t& template_id) const noexcept;

    uint32_t    prefix_ = 4;
    bool        passthrough_ = true;
    bool        venue_scope_ = false;
    FbDispatch  dispatch_ = FbDispatch::TYPE_FIELD;
    FbFieldPath type_field_;
    // type_field dispatch: direct index for 8-bit types, else linear scan over entries_.
    int16_t     by_u8_[256];
    std::vector<Entry> entries_;
    std::vector<std::string> warnings_;
};

} // namespace subplane
