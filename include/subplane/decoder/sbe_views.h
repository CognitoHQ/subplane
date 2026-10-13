#pragma once

// Typed views for flat SBE messages (no groups, no varData). COLD: tooling.
//
// A flat SBE message is a fixed block behind the message header, so the same
// bytes can be read as a C++ struct (when every field is naturally aligned) or
// a memcpy-accessor view (when not), as a NumPy structured dtype, and as a q
// `1:` fixed-width spec. The emitters below are driven by the schema the agent
// loads, so a view is never out of step with what the agent routes.
//
// Rules: little-endian schemas only; a message with groups or varData is
// skipped with a reason; constant fields are omitted and listed in a comment;
// enums and sets appear as their encoding primitive with the type name noted.

#include <string>
#include <vector>

#include "subplane/decoder/sbe_schema.h"

namespace subplane {

struct ViewsOptions {
    std::string              ns = "sp_views";  // C++ namespace
    std::vector<std::string> only;             // message names; empty = every message
};

struct ViewsReport {
    std::vector<std::string> emitted;          // message names
    std::vector<std::string> skipped;          // "Name: reason"
};

// One slot of a flat layout: header member, field or padding, in offset order.
struct ViewSlot {
    enum class Kind : uint8_t { HEADER, FIELD, PAD } kind = Kind::PAD;
    std::string name;        // field/header member name, or "_padN"
    uint32_t    offset = 0;  // from the start of the message (header included)
    uint32_t    size = 0;
    SbePrim     prim = SbePrim::U8;
    uint16_t    length = 1;  // CHAR_ARRAY / scalar-array element count
    SbePrim     elem = SbePrim::U8; // element primitive for scalar arrays (prim == OPAQUE)
    std::string type_name;
};

struct FlatLayout {
    std::string              name;
    uint16_t                 template_id = 0;
    uint32_t                 size = 0;          // header + block length
    bool                     aligned = true;    // every scalar at a multiple of its size
    std::string              misaligned_field;  // first offender, when !aligned
    std::vector<ViewSlot>    slots;
    std::vector<std::string> constants;         // omitted constant fields
};

// Builds the layout of one message; false with `reason` when it is not flat.
bool flat_layout(const SbeSchema& s, const MessageDef& m, FlatLayout& out, std::string& reason);

bool emit_cpp(const SbeSchema& s, const ViewsOptions& o, std::string& out, ViewsReport& rep, std::string& err);
bool emit_numpy(const SbeSchema& s, const ViewsOptions& o, std::string& out, ViewsReport& rep, std::string& err);
bool emit_q(const SbeSchema& s, const ViewsOptions& o, std::string& out, ViewsReport& rep, std::string& err);

// `package` attribute with non-alphanumerics replaced by '_' ("sample.md" -> "sample_md"); "schema" when empty.
std::string views_basename(const SbeSchema& s);

} // namespace subplane
