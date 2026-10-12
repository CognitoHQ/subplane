#pragma once

// Declarative packet framing.
//
// The only hard requirement on a bus publisher is its SBE schema. How SBE
// messages are laid out inside a datagram (packet header fields, per-message
// prefixes, where the sequence number lives) is described in YAML, compiled at
// activation into fixed offsets, and executed on the hot path with no
// allocation and no virtual calls. When no length prefix exists, message
// length comes from the schema walk (SbeLengthWalker).
//
// YAML (all keys optional):
//   framing:
//     packet_header:              # fields at the start of every packet, in order
//       - { name: session, type: bytes, size: 10 }
//       - { name: seq,     type: u64be }
//       - { name: count,   type: u16be }
//     message_prefix:             # fields before each message
//       - { name: len,     type: u16be }
//     sequence: packet_header.seq       # | message.<SchemaField> | none
//     count:    packet_header.count     # optional; bounds the message loop
//     length:   message_prefix.len      # | schema (default)
//     length_includes_prefix: false     # true if the length field counts its own prefix
//
// Any field may carry an explicit `offset:`; otherwise offsets accumulate.

#include <cstdint>
#include <string>
#include <vector>

#include <yaml-cpp/yaml.h>

#include "flashtick/framing/sbe_length.h"
#include "flashtick/transport/transport.h"

namespace flashtick {

enum class FieldType : uint8_t { U8, U16LE, U16BE, U32LE, U32BE, U64LE, U64BE, BYTES };

struct FramingField {
    std::string name;
    FieldType   type = FieldType::U8;
    uint32_t    offset = 0;
    uint32_t    size = 0;
};

enum class SeqSource : uint8_t { NONE, PACKET_HEADER, MESSAGE_FIELD };
enum class LenSource : uint8_t { SCHEMA, MESSAGE_PREFIX };

struct FramingConfig {
    std::vector<FramingField> packet_header;
    std::vector<FramingField> message_prefix;
    uint32_t packet_header_size = 0;
    uint32_t message_prefix_size = 0;

    SeqSource   seq_source = SeqSource::NONE;
    std::string seq_field;      // packet_header field name, or schema field name for MESSAGE_FIELD
    bool        has_count = false;
    std::string count_field;    // packet_header field name
    LenSource   len_source = LenSource::SCHEMA;
    std::string len_field;      // message_prefix field name
    bool        length_includes_prefix = false;

    // Parses the `framing:` block. An absent/null node yields the default
    // (no packet header, no prefix, length from schema, no sequence).
    static bool parse(const YAML::Node& node, FramingConfig& out, std::string& err);
};

struct FrameInfo {
    uint64_t seq = 0;
    bool     has_seq = false;
    uint32_t count = 0;        // declared message count, 0 if none
    bool     has_count = false;
};

struct MessageIter {
    const uint8_t* cur = nullptr;
    const uint8_t* end = nullptr;
    uint32_t index = 0;        // messages yielded so far
    uint32_t limit = 0;        // from count; 0 = unbounded
};

enum class NextStatus : uint8_t {
    OK,            // msg/len set
    END,           // no more messages
    TRUNCATED,     // prefix or declared length runs past the packet
    UNDECODABLE,   // length must come from the schema and the template is unknown: rest of packet dropped
    MALFORMED,     // schema walk found an inconsistent message
};

class Framer {
public:
    // Cold. walker may be null only when cfg.len_source == MESSAGE_PREFIX.
    bool init(const FramingConfig& cfg, const SbeLengthWalker* walker, std::string& err);

    // Hot. False if the packet is shorter than the packet header.
    bool parse(const PacketView& pkt, FrameInfo& info, MessageIter& it) const noexcept;

    // Hot. Yields the next message (SBE header at msg, total length len).
    NextStatus next(MessageIter& it, const uint8_t*& msg, uint32_t& len) const noexcept;

    SeqSource seq_source() const noexcept { return cfg_.seq_source; }
    const FramingConfig& config() const noexcept { return cfg_; }

private:
    struct Accessor {
        uint32_t offset = 0;
        uint8_t  size = 0;
        bool     big = false;
        bool     present = false;
    };
    static uint64_t read(const uint8_t* p, const Accessor& a) noexcept;

    FramingConfig cfg_;
    const SbeLengthWalker* walker_ = nullptr;
    Accessor seq_{}, count_{}, len_{};
};

} // namespace flashtick
