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

#include <cstdint>
#include <string>
#include <vector>

#include <yaml-cpp/yaml.h>

#include "subplane/core/message.h"
#include "subplane/decoder/decoder_common.h"

namespace subplane {

enum class FbScalar : uint8_t { NONE = 0, U8, U16, U32, U64, I8, I16, I32, I64, STRING };
bool parse_fb_scalar(std::string_view name, FbScalar& out);

struct FbFieldPath {
    static constexpr int kMaxDepth = 4;
    uint8_t  depth = 0;                 // 0 = not mapped
    uint16_t ids[kMaxDepth] = {};
    FbScalar type = FbScalar::NONE;
};

struct FbMessageSpec {
    uint32_t    type = 0;               // type_field dispatch
    char        identifier[4] = {};     // identifier dispatch
    bool        is_admin = false;
    uint8_t     msg_type = 0;           // MsgType or ControlCode
    FbFieldPath symbol, venue, exchange_ts, sequence;
};

enum class FbDispatch : uint8_t { TYPE_FIELD, IDENTIFIER };

struct FlatbuffersDecoderConfig {
    bool                       size_prefixed = true;
    SymbolIdMode               symbol_mode = SymbolIdMode::PASSTHROUGH;
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

    const std::vector<std::string>& warnings() const { return warnings_; }
    bool symbol_passthrough() const noexcept { return passthrough_; }
    bool size_prefixed() const noexcept { return prefix_ == 4; }

private:
    struct Entry {
        bool        present = false;
        bool        admin = false;
        uint8_t     msg_type = 0;
        uint16_t    template_id = 0;
        char        identifier[4] = {};
        FbFieldPath symbol, venue, exchange_ts, sequence;
    };
    const Entry* find(const uint8_t* msg, uint32_t avail, uint32_t root, uint16_t& template_id) const noexcept;

    uint32_t    prefix_ = 4;
    bool        passthrough_ = true;
    FbDispatch  dispatch_ = FbDispatch::TYPE_FIELD;
    FbFieldPath type_field_;
    // type_field dispatch: direct index for 8-bit types, else linear scan over entries_.
    int16_t     by_u8_[256];
    std::vector<Entry> entries_;
    std::vector<std::string> warnings_;
};

} // namespace subplane
