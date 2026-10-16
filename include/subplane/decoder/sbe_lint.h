#pragma once

// Schema lint: the mechanical part of subplane's schema rules. COLD: tooling
// (`subplane schema --lint`), used by the subplane-schema-designer skill.
//
// Two profiles:
// - data (default): a bus schema. Messages should be flat and naturally
//   aligned (struct views, cheap routing), carry the routing fields at the
//   same root offsets in every data message, use integer instruments and
//   fixed-point prices.
// - control (`--control`): an SPP-style schema. Every root starts with a u64
//   CorrelationId; groups and varData are fine.
// With a baseline (`--against old.xml`), changes must follow SBE's
// append-only evolution, as docs/protocol/subplane-protocol.md states.

#include <string>
#include <vector>

#include "subplane/decoder/sbe_schema.h"

namespace subplane {

struct LintOptions {
    bool control = false;
    // Data profile: field names every data message must carry at one shared offset.
    std::vector<std::string> routing_fields;
    // Data profile: template ids that are admin or control, exempt from the data rules.
    std::vector<uint16_t> admin_ids;
    const SbeSchema* baseline = nullptr;
};

struct LintFinding {
    enum class Level : uint8_t { ERROR, WARNING } level = Level::WARNING;
    std::string where;   // "Message.Field" or "schema"
    std::string message;
};

std::vector<LintFinding> lint_schema(const SbeSchema& s, const LintOptions& o);

} // namespace subplane
