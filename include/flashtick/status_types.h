#pragma once

// Cold-path DTOs exposed by the HTTP API. Filled by the Agent, serialized by ApiServer.

#include <cstdint>
#include <string>
#include <vector>

namespace flashtick {

struct PartitionStatus {
    std::string name;
    uint16_t    id = 0;
    std::string state;          // "init", "active", "degraded", "gap"
    uint64_t    last_sequence = 0;
    uint64_t    packets = 0;
    uint64_t    messages = 0;
    uint64_t    unknown_templates = 0;
    uint64_t    gaps = 0;
    uint64_t    last_rx_ns = 0;
};

struct SessionStatus {
    std::string name;
    uint32_t    id = 0;
    std::string owner;          // "config" or "client"
    std::string ring_path;
    uint64_t    epoch = 0;
    uint64_t    published = 0;
    uint64_t    filtered = 0;
    uint32_t    filter_generation = 0;
};

struct AgentStatus {
    std::string name;
    std::string version;
    bool        running = false;
    uint64_t    uptime_seconds = 0;
    uint64_t    epoch = 0;
    std::vector<PartitionStatus> partitions;
    std::vector<SessionStatus>   sessions;
};

} // namespace flashtick
