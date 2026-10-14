#pragma once

// Types shared by every decoder component. A decoder fills the routing
// fields of a TickHeader from one message and reports the message's total
// length; it never fully decodes. Which decoder a partition uses is YAML
// configuration (`decoder.type`), independent of the transport.

#include <cstdint>
#include <string>
#include <string_view>

#include "subplane/core/message.h"

namespace subplane {

enum class SymbolIdMode { TABLE, PASSTHROUGH };

enum class DecodeStatus : uint8_t { OK, UNKNOWN_TEMPLATE, TRUNCATED, SCHEMA_MISMATCH, MALFORMED };

struct DecodeResult {
    DecodeStatus status    = DecodeStatus::MALFORMED;
    uint32_t     total_len = 0; // full message length; 0 when unknown
};

// Minimal interface so decoders do not depend on the SymbolTable.
// TODO: replace with a template parameter / direct SymbolTable call; one
// virtual call per message is accepted for now.
class SymbolResolver {
public:
    virtual ~SymbolResolver() = default;
    // `venue` is 0 unless the decoder scopes integer ids by venue (symbol_scope: venue).
    virtual uint32_t resolve_u64(uint64_t key, uint16_t venue) noexcept = 0;
    virtual uint32_t resolve_str(const char* s, uint32_t len) noexcept = 0;
};

// Config vocabulary shared by all decoders: data message types and bus admin types.
bool parse_msg_type_name(std::string_view name, uint8_t& code);   // TRADE, QUOTE, ...
bool parse_admin_name(std::string_view name, uint8_t& code);      // HEARTBEAT, SEQ_RESET, ...
bool parse_symbol_id_mode(std::string_view name, SymbolIdMode& out);
// symbol_scope: global (default) | venue. Venue scope interns an integer symbol
// together with the message's venue, so the same id from two venues is two
// symbols. Table mode and integer symbols only; the venue field must be mapped.
bool parse_symbol_scope(std::string_view name, bool& venue_scope);

} // namespace subplane
