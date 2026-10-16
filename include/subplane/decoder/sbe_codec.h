#pragma once

// Header-only C++ encoders and decoders generated from an SBE schema. COLD:
// tooling (`subplane schema --codec cpp`).
//
// Unlike the typed views (sbe_views.h) these handle repeating groups and
// variable-length data, so they serve control schemas such as the subplane
// protocol (schemas/subplane_proto.xml). Per message:
// - an encoder over a caller buffer: root setters at any time, then groups
//   and data in schema order; elements skipped are written empty; overflow is
//   sticky; finish() returns the message length (0 on any error);
// - a decoder: wrap() checks the header and every length against the buffer
//   once; getters respect the received blockLength and version, so a field a
//   sender does not have yet reads as 0 / empty.
//
// Supported: little-endian, the standard 8-byte header, scalar and char-array
// fields (enums typed), one level of groups with fixed entries, varData after
// the groups. Anything else is refused with a reason.

#include <string>
#include <vector>

#include "subplane/decoder/sbe_schema.h"

namespace subplane {

struct CodecOptions {
    std::string ns = "sp_codec";   // C++ namespace
};

// False with `err` when the schema uses something outside the supported set.
bool emit_cpp_codec(const SbeSchema& s, const CodecOptions& o, std::string& out, std::string& err);

} // namespace subplane
