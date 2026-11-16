#pragma once

// Cold-path DTOs exposed by the HTTP API. Filled by the Agent, serialized by ApiServer.

#include <cstdint>
#include <string>
#include <vector>

namespace subplane {

struct PartitionStatus {
    std::string name;           // "<topic>/<partition_num>"
    std::string topic;
    uint16_t    topic_id = 0;
    uint16_t    partition_num = 0;
    int32_t     stream_id = 0;
    std::string state;          // "init", "active", "degraded", "gap"
    uint64_t    last_sequence = 0;
    uint64_t    packets = 0;
    uint64_t    messages = 0;
    uint64_t    unknown_templates = 0;
    uint64_t    gaps = 0;
    uint64_t    last_rx_ns = 0;
    std::string transport;      // "unknown", "up", "down"
    uint64_t    transport_down = 0;
    uint64_t    transport_up = 0;
    uint64_t    stream_mismatch = 0;    // heartbeats stamped with another stream_id
    int32_t     last_stream_stamp = 0;  // the last such stamp
    uint64_t    inst_gaps = 0;          // instrument sequence jumps (INSTRUMENT_GAP)
    uint64_t    inst_dups = 0;          // records dropped as an instrument's duplicate
    std::string recap_json;     // recap counters as JSON; empty without a recap block
    std::string resolver_json;  // resolver counters as JSON; empty except on a topic's partition 1 with a resolver
    // tap: (empty tap_ring without one): the ring a recorder reads, what went
    // into it, and the slowest reader's lag behind the head.
    std::string tap_ring;
    uint64_t    tap_records = 0;
    uint64_t    tap_dropped = 0;
    uint64_t    tap_bytes = 0;      // ring head
    uint32_t    tap_readers = 0;
    uint64_t    tap_lag_bytes = 0;
};

// One reader's slot as the agent last read it (shm/ring_layout.h). Cold copy;
// the contents come from the app and are reported, never acted on.
struct ReaderReport {
    uint32_t    slot = 0;
    int32_t     pid = 0;
    uint64_t    start_time = 0;
    uint32_t    uid = 0;
    std::string name;                 // SUBPLANE_APP_NAME, may be empty
    bool        identity_ready = false;
    uint32_t    sdk_version = 0;
    uint32_t    capabilities = 0;
    uint32_t    flags_requested = 0;  // ring-wide | per-slot flags the agent set
    uint32_t    flags_active = 0;     // what the reader applies
    uint64_t    read_pos = 0;
    uint64_t    heartbeat_ns = 0;
    uint64_t    overruns = 0;
    uint64_t    records = 0;
    uint64_t    lat_buckets[38] = {}; // READER_LAT_BUCKETS
    uint64_t    lat_sum_ns = 0;
    uint64_t    lat_count = 0;
};

struct SessionStatus {
    std::string name;
    uint32_t    id = 0;
    std::string owner;          // "config" or "client"
    std::string ring_path;
    uint64_t    epoch = 0;
    uint64_t    published = 0;
    uint64_t    filtered = 0;
    uint64_t    publish_failed = 0;
    uint64_t    ring_overruns = 0;
    uint32_t    filter_generation = 0;
    uint64_t    ring_head = 0;          // bytes written to the ring since it opened
    uint64_t    ring_capacity = 0;
    uint64_t    heartbeat_ns = 0;       // the writer's last ring heartbeat (CLOCK_REALTIME), 0 if none
    std::vector<std::string> topics;     // its topic patterns, as given (empty = all)
    std::vector<ReaderReport> readers;   // claimed reader slots
    uint32_t    reader_flags = 0;        // ring-wide flags (READER_FLAG_*)
    uint32_t    reader_sample_every = 0;
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

} // namespace subplane
