#pragma once

// SBE routing decoder: extracts only the routing fields (symbol, sequence,
// exchange timestamp, venue) from the root block of each message using an
// offset table built from the schema at init. Apps get the original SBE bytes;
// flashtick never fully decodes.

#include <cstdint>
#include <string>
#include <vector>

#include <yaml-cpp/yaml.h>

#include "flashtick/core/message.h"
#include "flashtick/decoder/sbe_schema.h"
#include "flashtick/framing/sbe_length.h"

namespace flashtick {

struct RoutingFieldMap { std::string symbol, sequence, exchange_ts, venue; }; // empty = unmapped
enum class SymbolIdMode { TABLE, PASSTHROUGH };

struct TemplateMapping {
    uint16_t template_id = 0;
    bool     is_admin    = false;
    uint8_t  msg_type    = 0;  // MsgType (data)
    uint8_t  admin_code  = 0;  // ControlCode (admin)
};

struct DecoderConfig {
    std::string                  schema_path;
    RoutingFieldMap              fields;
    SymbolIdMode                 symbol_mode = SymbolIdMode::TABLE;
    std::vector<TemplateMapping> messages;
};

// Parses the config.yaml `decoder:` block.
bool parse_decoder_config(const YAML::Node& node, DecoderConfig& out, std::string& err);

enum class DecodeStatus : uint8_t { OK, UNKNOWN_TEMPLATE, TRUNCATED, SCHEMA_MISMATCH, MALFORMED };

struct DecodeResult {
    DecodeStatus status    = DecodeStatus::MALFORMED;
    uint32_t     total_len = 0; // full message length via the walker; 0 when unknown
};

// Minimal interface so the decoder does not depend on the SymbolTable.
// TODO: replace with a template parameter / direct SymbolTable call; one
// virtual call per message is accepted for now.
class SymbolResolver {
public:
    virtual ~SymbolResolver() = default;
    virtual uint32_t resolve_u64(uint64_t key) noexcept = 0;
    virtual uint32_t resolve_str(const char* s, uint32_t len) noexcept = 0;
};

class SbeRoutingDecoder {
public:
    // Cold. Builds the dense by-template table and the walker.
    bool init(const DecoderConfig& cfg, const SbeSchema& schema, std::string& err);

    // Hot. No heap, no exceptions. Does not touch payload_len/ingest_ts/partition_id/seq_tag/flags.
    DecodeResult decode(const uint8_t* msg, uint32_t avail, TickHeader& out, SymbolResolver& syms) const noexcept;

    const SbeLengthWalker& walker() const noexcept { return walker_; }
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
        const WalkProgram* prog = nullptr;
        FieldRef symbol, sequence, exchange_ts, venue;
    };

    std::vector<Entry>  by_template_;
    SbeLengthWalker     walker_;
    HeaderLayout        hdr_;
    bool                little_ = true;
    bool                passthrough_ = false;
    uint16_t            schema_id_ = 0;
    std::vector<std::string> warnings_;
};

} // namespace flashtick
