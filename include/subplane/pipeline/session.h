#pragma once

// A session is one app subscription: a filter table and a shared-memory ring.
// Sessions are created on the cold path (YAML or control channel) and attached
// to the pipeline thread through commands; only the pipeline thread writes to
// the ring (single-writer invariant).

#include "subplane/core/counters.h"
#include "subplane/core/filter_table.h"
#include "subplane/fanout/ring_writer.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <yaml-cpp/yaml.h>

namespace subplane {

// Cold description of a session (from YAML or the control channel).
struct SessionSpec {
    std::string name;
    std::string owner = "config";          // "config" | "client"
    uint64_t    ring_capacity_bytes = 64ull << 20;
    uint32_t    max_payload = 4096;
    std::vector<std::string> topics;       // topic patterns (`*` one segment, final `>` the rest); empty = all
    std::vector<std::string> symbols;      // names; "*" = all
    bool        all_symbols = false;
    std::vector<std::string> msg_types;    // TRADE, QUOTE, ...; empty = all data types
    bool        bus_admin = true;
    bool        agent_control = true;
    // Ask for recap: each symbol starts with the snapshot service's state, then
    // live data after it (architecture.md, "Recap"). Fixed at creation.
    bool        recap = false;

    // Parses one entry of the `sessions:` sequence.
    static bool parse(const YAML::Node& node, SessionSpec& out, std::string& err);
};

// Data msg type names <-> codes.
bool        msg_type_from_name(std::string_view name, uint8_t& code);
const char* msg_type_name(uint8_t code);

struct Session {
    uint32_t    id = 0;
    std::string name;
    std::string owner;
    uint64_t    partition_mask = 0;        // agent-local: the partitions of its topics it is attached to
    bool        recap = false;             // fixed at creation (SessionSpec::recap)
    uint16_t    recap_slot = 0;            // pipeline thread: this session's waiter bit (RecapWaiters)

    // INTENT: the pipeline thread reads `filter` with acquire on every message
    // and never frees it; the cold side allocates a new table, posts SWAP_FILTER,
    // waits for `applied_generation` to reach the new generation, then frees the
    // old one. Freeing before the ack is a use-after-free on the hot path.
    std::atomic<const FilterTable*> filter{nullptr};
    std::atomic<uint32_t>           applied_generation{0};
    std::unique_ptr<FilterTable>    filter_owned;    // current (cold side)
    std::unique_ptr<FilterTable>    filter_retiring; // previous, freed after ack

    RingWriter      ring;
    SessionCounters counters;
    std::atomic<bool> attached{false}; // set by the pipeline thread
};

} // namespace subplane
