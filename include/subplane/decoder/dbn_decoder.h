#pragma once

// DBN (Databento Binary Encoding) routing decoder. Every DBN record starts
// with a fixed 16-byte header:
//     u8  length        total record bytes / 4
//     u8  rtype         record type
//     u16 publisher_id  dataset+venue id  -> venue
//     u32 instrument_id                   -> symbol
//     u64 ts_event      exchange ns       -> exchange_ts
// so routing needs no schema: offsets are constants. The per-rtype sequence
// field (where DBN carries one) is read from a built-in table. Apps get the
// original record bytes; subplane never decodes the body.
//
// Which rtypes map to which subplane msg types is YAML (`messages:`), with a
// sensible default when omitted. The framer must hand exactly one record
// (`framing.length: packet`); the record's own length byte is validated.

#include <cstdint>
#include <string>
#include <vector>

#include <yaml-cpp/yaml.h>

#include "subplane/core/message.h"
#include "subplane/decoder/decoder_common.h"

namespace subplane {

struct DbnRtypeInfo {
    const char* name;
    uint8_t     code;
    uint16_t    sequence_off; // 0 = record has no sequence field
};
// Built-in rtype table (name, code, sequence offset). Cold.
const DbnRtypeInfo* dbn_rtype_by_name(std::string_view name);
const DbnRtypeInfo* dbn_rtype_by_code(uint8_t code);

struct DbnMapping {
    uint8_t rtype      = 0;
    bool    is_admin   = false;
    uint8_t msg_type   = 0;   // MsgType (data)
    uint8_t admin_code = 0;   // ControlCode (admin)
};

struct DbnDecoderConfig {
    SymbolIdMode            symbol_mode = SymbolIdMode::PASSTHROUGH;
    bool                    venue_scope = false;   // symbol_scope: venue (venue = publisher_id)
    std::vector<DbnMapping> messages;   // empty = defaults (see dbn_default_mappings)
};

// Parses the config.yaml `decoder:` block (type dbn).
bool parse_dbn_decoder_config(const YAML::Node& node, DbnDecoderConfig& out, std::string& err);
std::vector<DbnMapping> dbn_default_mappings();

class DbnDecoder {
public:
    static constexpr uint32_t kHeaderSize = 16;

    // Cold.
    bool init(const DbnDecoderConfig& cfg, std::string& err);

    // Hot. `avail` must be the whole record: the length byte is checked against it.
    DecodeResult decode(const uint8_t* msg, uint32_t avail, TickHeader& out, SymbolResolver& syms) const noexcept;

    const std::vector<std::string>& warnings() const { return warnings_; }
    bool symbol_passthrough() const noexcept { return passthrough_; }
    bool symbol_venue_scope() const noexcept { return venue_scope_; }

private:
    struct Entry {
        bool     present = false;
        bool     admin   = false;
        uint8_t  msg_type = 0;
        uint16_t sequence_off = 0;
    };
    Entry by_rtype_[256] = {};
    bool  passthrough_ = true;
    bool  venue_scope_ = false;
    std::vector<std::string> warnings_;
};

} // namespace subplane
