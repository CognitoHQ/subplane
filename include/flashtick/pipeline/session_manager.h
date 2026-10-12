#pragma once

// Cold-side owner of sessions. Builds filter tables, opens rings, posts
// commands to the owning pipeline thread and waits for acks, reconciles the
// YAML `sessions` section on reload, and expires client-owned sessions after
// their socket closes. All public methods are thread-safe (one mutex); none
// of this runs on the hot path.

#include "flashtick/pipeline/lvc.h"
#include "flashtick/pipeline/partition.h"
#include "flashtick/pipeline/pipeline.h"
#include "flashtick/pipeline/session.h"
#include "flashtick/status_types.h"
#include "flashtick/symbol_table.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace flashtick {

struct SessionManagerConfig {
    std::string ring_dir = "/dev/shm";
    std::string agent_name = "flashtick";
    uint64_t    epoch = 0;
    uint32_t    max_symbols = 65536;
    uint64_t    linger_ns = 5'000'000'000ull;  // client sessions survive this long after disconnect
    uint64_t    ack_timeout_ns = 2'000'000'000ull;
};

struct SessionAttachInfo {
    uint32_t    id = 0;
    std::string name;
    std::string owner;
    std::string ring_path;
    uint64_t    epoch = 0;
    uint64_t    capacity = 0;
    uint32_t    max_payload = 0;
    uint32_t    filter_generation = 0;
};

class SessionManager {
public:
    SessionManager(SessionManagerConfig cfg, SymbolTable& symbols, Lvc& lvc,
                   std::vector<Partition*> partitions, std::vector<Pipeline*> groups);
    ~SessionManager();

    // Creates and attaches. Fails on name collision or unknown partition/symbol constraints.
    bool create(const SessionSpec& spec, std::string& err);
    // Rebuilds the filter from spec (name must exist), swaps it, frees the old one after the ack.
    bool set_filter(const SessionSpec& spec, std::string& err);
    // Detaches, closes and unlinks the ring, frees. reason: 0 shutdown, 1 destroyed, 2 reload.
    bool destroy(const std::string& name, uint32_t reason, std::string& err);
    // Applies the YAML sessions section: create new, set_filter changed, destroy missing (owner=config only).
    bool reconcile_config(const std::vector<SessionSpec>& specs, std::string& err);
    // Destroys every session (shutdown).
    void destroy_all();

    bool attach_info(const std::string& name, SessionAttachInfo& out) const;
    std::vector<SessionStatus> status() const;
    size_t count() const;

    // Client-owned lifecycle: called by the control server on disconnect; service_linger() destroys expired ones.
    void client_disconnected(const std::string& name, uint64_t now_ns);
    void client_reattached(const std::string& name);
    void service_linger(uint64_t now_ns);

    // Last-value snapshot for a session: every (symbol, msg_type) its filter accepts.
    // Returns the ring head observed before iterating so the client can seek to it.
    using SnapshotFn = std::function<void(const TickHeader&, const uint8_t*, uint32_t)>;
    bool snapshot(const std::string& name, uint64_t& ring_head_at_snapshot, const SnapshotFn& fn) const;

    // Resolves a spec into a FilterSpec + partition mask + owning group. Exposed for tests.
    bool resolve(const SessionSpec& spec, FilterSpec& fs, uint64_t& partition_mask, Pipeline*& group, std::string& err) const;

private:
    struct Entry {
        std::unique_ptr<Session> session;
        SessionSpec spec;
        Pipeline*   group = nullptr;
        uint32_t    generation = 0;
        bool        client_connected = false;
        uint64_t    disconnect_ns = 0;
    };

    Entry* find_locked(const std::string& name);
    const Entry* find_locked(const std::string& name) const;
    bool destroy_locked(Entry& e, uint32_t reason, std::string& err);
    std::string ring_path(const std::string& name) const;
    bool post_and_wait(Pipeline& g, const PipelineCommand& c, std::string& err);
    bool specs_differ(const SessionSpec& a, const SessionSpec& b) const;

    SessionManagerConfig cfg_;
    SymbolTable& symbols_;
    Lvc& lvc_;
    std::vector<Partition*> partitions_;
    std::vector<Pipeline*> groups_;
    mutable std::mutex mutex_;
    std::vector<Entry> entries_;
    uint32_t next_id_ = 1;
};

} // namespace flashtick
