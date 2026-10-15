#pragma once

// SBE routing decoder: extracts only the routing fields (symbol, sequence,
// exchange timestamp, venue) from the root block of each message using an
// offset table built from the schema at init. Apps get the original SBE bytes;
// subplane never fully decodes.

#include <cstdint>
#include <string>
#include <vector>

#include <yaml-cpp/yaml.h>

#include "subplane/core/message.h"
#include "subplane/decoder/decoder_common.h"
#include "subplane/decoder/sbe_schema.h"
#include "subplane/framing/sbe_length.h"

namespace subplane {

struct RoutingFieldMap { std::string symbol, sequence, exchange_ts, venue; }; // empty = unmapped

struct TemplateMapping {
    uint16_t template_id = 0;
    bool     is_admin    = false;
    uint8_t  msg_type    = 0;  // MsgType (data)
    uint8_t  admin_code  = 0;  // ControlCode (admin)
    RecapMarker marker   = RecapMarker::NONE;   // admin: RECAP_BEGIN | RECAP_END | RECAP_REJECT
    std::string recap_fields[kRecapFields];     // root-block field names from `fields:` (markers only)
};

struct DecoderConfig {
    std::string                  schema_path;
    RoutingFieldMap              fields;
    SymbolIdMode                 symbol_mode = SymbolIdMode::TABLE;
    bool                         venue_scope = false;   // symbol_scope: venue
    std::vector<TemplateMapping> messages;
};

// Parses the config.yaml `decoder:` block (type sbe_routing).
bool parse_decoder_config(const YAML::Node& node, DecoderConfig& out, std::string& err);

class SbeRoutingDecoder {
public:
    // Cold. Builds the dense by-template table and the walker.
    bool init(const DecoderConfig& cfg, const SbeSchema& schema, std::string& err);

    // Hot. No heap, no exceptions. Does not touch payload_len/ingest_ts/partition_id/seq_tag/flags.
    DecodeResult decode(const uint8_t* msg, uint32_t avail, TickHeader& out, SymbolResolver& syms) const noexcept;

    // Cold side of the recap merge (not the live hot path). Reads a marker's
    // mapped fields. False when the message is not a mapped recap marker.
    bool read_recap_fields(const uint8_t* msg, uint32_t avail, RecapMarkerFields& out) const noexcept;

    const SbeLengthWalker& walker() const noexcept { return walker_; }
    bool symbol_passthrough() const noexcept { return passthrough_; }
    bool symbol_venue_scope() const noexcept { return venue_scope_; }
    const std::vector<std::string>& warnings() const { return warnings_; }

private:
    struct FieldRef {
        uint16_t offset = 0;
        uint16_t end    = 0;   // offset+size; 0 = unmapped / absent in this template
        uint8_t  size   = 0;
        bool     is_str = false;
    };
    struct Entry {
        bool     present = false;
        bool     admin   = false;
        uint8_t  msg_type = 0;
        RecapMarker marker = RecapMarker::NONE;
        const WalkProgram* prog = nullptr;
        FieldRef symbol, sequence, exchange_ts, venue;
        FieldRef recap[kRecapFields];
    };

    std::vector<Entry>  by_template_;
    SbeLengthWalker     walker_;
    HeaderLayout        hdr_;
    bool                little_ = true;
    bool                passthrough_ = false;
    bool                venue_scope_ = false;
    uint16_t            schema_id_ = 0;
    std::vector<std::string> warnings_;
};

} // namespace subplane
