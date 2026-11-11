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

// Recap reply markers: messages of the stream's own schema that open, close or
// reject a snapshot-service reply. Mapped in decoder config like admin
// messages (`admin: RECAP_BEGIN`), but consumed by the agent, never delivered
// to apps as-is.
enum class RecapMarker : uint8_t { NONE = 0, BEGIN = 1, END = 2, REJECT = 3 };

// Fields a marker carries, by index into RecapMarkerFields.
enum RecapField : uint8_t { RECAP_REQUEST_ID = 0, RECAP_AS_OF = 1, RECAP_SOURCE_EPOCH = 2, RECAP_REASON = 3, kRecapFields = 4 };

struct RecapMarkerFields {
    RecapMarker marker = RecapMarker::NONE;
    uint64_t    value[kRecapFields] = {};
    bool        has[kRecapFields] = {};
};

struct DecodeResult {
    DecodeStatus status    = DecodeStatus::MALFORMED;
    uint32_t     total_len = 0; // full message length; 0 when unknown
    RecapMarker  marker    = RecapMarker::NONE; // set on OK when the message is a recap marker
    int32_t      stream_stamp = 0;  // an admin message's stream_id field; 0 = not stamped
    uint64_t     instrument_seq = 0; // a data message's instrument_sequence field; 0 = not counted or unmapped
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
// RECAP_BEGIN, RECAP_END, RECAP_REJECT (written as `admin:` names).
bool parse_recap_marker_name(std::string_view name, RecapMarker& out);
// request_id, as_of, source_epoch, reason (keys of a marker's `fields:`).
bool parse_recap_field_name(std::string_view name, RecapField& out);
const char* recap_field_name(RecapField f);
// Cold. nullptr when `has` holds every field the marker requires (request_id
// always, as_of on RECAP_BEGIN), else the name of the first missing one.
const char* recap_missing_field(RecapMarker m, const bool (&has)[kRecapFields]);
// symbol_scope: global (default) | venue. Venue scope interns an integer symbol
// together with the message's venue, so the same id from two venues is two
// symbols. Table mode and integer symbols only; the venue field must be mapped.
bool parse_symbol_scope(std::string_view name, bool& venue_scope);

} // namespace subplane
