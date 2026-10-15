#pragma once

// Manager keys of a partition's `recap:` block. The component keys (type,
// channels) are parsed by the component selected by `type`.
//
//   recap:
//     type: aeron
//     ...component keys...
//     timeout_ms: 2000
//     max_inflight: 64                     # outstanding requests per partition; more wait in FIFO order
//     retry: { base_ms: 50, max_ms: 5000 } # exponential backoff with full jitter
//     reply_slabs: 8                       # preallocated reply buffers per partition
//     reply_slab_bytes: 1048576            # the largest reply the agent accepts

#include <cstdint>
#include <string>

#include <yaml-cpp/yaml.h>

namespace subplane {

struct RecapConfig {
    std::string type;
    uint32_t    timeout_ms = 2000;
    uint32_t    max_inflight = 64;
    uint32_t    retry_base_ms = 50;
    uint32_t    retry_max_ms = 5000;
    uint32_t    reply_slabs = 8;
    uint32_t    reply_slab_bytes = 1u << 20;

    static bool parse(const YAML::Node& node, RecapConfig& out, std::string& err);
};

} // namespace subplane
