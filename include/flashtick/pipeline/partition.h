#pragma once

// A partition is one activation: transport subscription + framing + decoder
// for one bus partition. Built on the cold path from YAML; its hot state
// (sequence tracking, counters, attached sessions) is owned by exactly one
// pipeline thread.

#include "flashtick/core/counters.h"
#include "flashtick/decoder/sbe_routing_decoder.h"
#include "flashtick/decoder/sbe_schema.h"
#include "flashtick/framing/framing.h"
#include "flashtick/transport/transport.h"

#include <cstdint>
#include <memory>
#include <string>

#include <yaml-cpp/yaml.h>

namespace flashtick {

struct Session;

struct PartitionConfig {
    std::string name;
    uint16_t    id = 0;
    std::string transport_type;
    YAML::Node  transport_cfg;
    YAML::Node  framing_node;
    YAML::Node  decoder_node;
    std::string base_dir;             // for resolving relative schema paths
    uint32_t    heartbeat_timeout_ms = 2000;
    uint64_t    dup_window = 1024;

    // Parses one entry of the `partitions:` sequence. `id` is the index.
    static bool parse(const YAML::Node& node, uint16_t id, const std::string& base_dir,
                      PartitionConfig& out, std::string& err);
};

enum class PartitionState : uint8_t { INIT = 0, ACTIVE = 1, DEGRADED = 2 };
const char* partition_state_name(PartitionState s);

class Partition {
public:
    static constexpr uint32_t kMaxSessions = 256;

    // Cold. Loads the schema, builds decoder + framer, constructs the transport
    // via the registry (or uses `transport_override` when given, for tests).
    bool build(const PartitionConfig& cfg, std::string& err,
               std::unique_ptr<Transport> transport_override = nullptr);
    bool open(std::string& err);   // opens the transport
    void close() noexcept;

    // Cold accessors.
    const std::string& name() const noexcept { return cfg_.name; }
    uint16_t id() const noexcept { return cfg_.id; }
    uint64_t heartbeat_timeout_ns() const noexcept { return static_cast<uint64_t>(cfg_.heartbeat_timeout_ms) * 1'000'000ull; }
    const SbeSchema& schema() const noexcept { return schema_; }
    const SbeRoutingDecoder& decoder() const noexcept { return decoder_; }
    const Framer& framer() const noexcept { return framer_; }
    Transport& transport() noexcept { return *transport_; }
    bool has_transport() const noexcept { return transport_ != nullptr; }
    const PartitionConfig& config() const noexcept { return cfg_; }

    // Hot state (pipeline thread only, except counters which are read cold).
    PartitionCounters counters;
    uint64_t          expected_seq = 0;      // 0 = unknown, resync on next
    // Backward jumps smaller than this are duplicates; larger ones mean the
    // publisher restarted its sequence and we resync (counted as seq_resets).
    uint64_t          dup_window = 1024;
    uint64_t          last_seq = 0;
    uint64_t          last_rx_ns = 0;
    PartitionState    state = PartitionState::INIT;
    Session*          sessions[kMaxSessions] = {};
    uint32_t          n_sessions = 0;

    // Hot. Returns false if full or already attached.
    bool attach(Session* s) noexcept;
    bool detach(Session* s) noexcept;

    // Cold snapshot for /partitions.
    std::atomic<uint8_t>  state_pub{0};
    std::atomic<uint64_t> last_seq_pub{0};
    std::atomic<uint64_t> last_rx_pub{0};

private:
    PartitionConfig            cfg_;
    SbeSchema                  schema_;
    SbeRoutingDecoder          decoder_;
    Framer                     framer_;
    std::unique_ptr<Transport> transport_;
};

} // namespace flashtick
