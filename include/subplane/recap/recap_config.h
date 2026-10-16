#pragma once

// Manager keys of a partition's `recap:` block. The component keys (type,
// channels) are parsed by the component selected by `type`.
//
//   recap:
//     type: aeron
//     ...component keys...
//     reply: spp                           # spp (default): replies are SPP RecapBegin/RecapRecord/RecapEnd;
//                                          # stream_markers: replies in the stream's own schema between
//                                          # decoder-mapped RECAP_BEGIN / RECAP_END markers
//     timeout_ms: 2000
//     max_inflight: 64                     # outstanding requests per partition; more wait in FIFO order
//     retry: { base_ms: 50, max_ms: 5000 } # exponential backoff with full jitter
//     reply_slabs: 8                       # preallocated reply buffers per partition
//     reply_slab_bytes: 1048576            # the largest reply the agent accepts
//     max_pending: 4096                    # symbols pending at once (pipeline side); sessions are refused beyond it
//     hold_bytes: 4194304                  # live messages held for pending symbols, per partition
//     hold_chunk_bytes: 256                # allocation unit of the hold arena

#include <cstdint>
#include <string>

#include <yaml-cpp/yaml.h>

namespace subplane {

enum class RecapReplyMode : uint8_t { SPP, STREAM_MARKERS };

struct RecapConfig {
    std::string type;
    RecapReplyMode reply = RecapReplyMode::SPP;
    uint32_t    timeout_ms = 2000;
    uint32_t    max_inflight = 64;
    uint32_t    retry_base_ms = 50;
    uint32_t    retry_max_ms = 5000;
    uint32_t    reply_slabs = 8;
    uint32_t    reply_slab_bytes = 1u << 20;
    uint32_t    max_pending = 4096;
    uint64_t    hold_bytes = 4ull << 20;
    uint32_t    hold_chunk_bytes = 256;

    static bool parse(const YAML::Node& node, RecapConfig& out, std::string& err);
};

} // namespace subplane
