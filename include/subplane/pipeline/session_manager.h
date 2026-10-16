#pragma once

// Cold-side owner of sessions. Builds filter tables, opens rings, posts
// commands to the owning pipeline thread and waits for acks, reconciles the
// YAML `sessions` section on reload, and expires client-owned sessions after
// their socket closes. All public methods are thread-safe (one mutex); none
// of this runs on the hot path.

#include "subplane/pipeline/lvc.h"
#include "subplane/pipeline/partition.h"
#include "subplane/pipeline/pipeline.h"
#include "subplane/pipeline/session.h"
#include "subplane/status_types.h"
#include "subplane/symbol_table.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace subplane {

struct SessionManagerConfig {
    std::string ring_dir = "/dev/shm";
    std::string agent_name = "subplane";
    uint64_t    epoch = 0;
    uint32_t    max_symbols = 65536;
    uint64_t    linger_ns = 5'000'000'000ull;  // client sessions survive this long after disconnect
    uint64_t    ack_timeout_ns = 2'000'000'000ull;
    uint32_t    ring_mode = 0644;                  // agent.ring_mode: 0660 lets a shared group claim reader slots
    uint64_t    reader_stale_ns = 30'000'000'000ull; // a dead reader's slot is reclaimed after this long without a heartbeat
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

    // Reader slots, cold. Frees the slots of readers that are dead (pid gone,
    // or reused with another start time) and silent for reader_stale_ns.
    // Rate-limited to once a second; returns the slots freed.
    uint32_t service_readers(uint64_t now_ns);
    // Latency tracing for a session's readers: every reader (pid 0, the
    // ring-wide flag) or the slots of one pid. `sample_every` 0 keeps 1 in 1.
    // Returns false with err when the session does not exist, or when `pid`
    // holds none of its slots; `matched` is the number of slots changed.
    bool set_reader_trace(const std::string& session, bool on, int32_t pid, uint32_t sample_every, uint32_t& matched,
                          std::string& err);

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
        std::vector<uint32_t> recap_symbols; // filter symbol ids of a recap session
    };

    Entry* find_locked(const std::string& name);
    const Entry* find_locked(const std::string& name) const;
    bool destroy_locked(Entry& e, uint32_t reason, std::string& err);
    std::string ring_path(const std::string& name) const;
    bool post_and_wait(Pipeline& g, const PipelineCommand& c, std::string& err);
    bool specs_differ(const SessionSpec& a, const SessionSpec& b) const;
    // Recap sessions: a symbol list (not all symbols), agent control events on,
    // and the distinct symbols of all recap sessions on each recap partition
    // within its recap.max_pending, so the pipeline's pending table cannot fill.
    bool check_recap_locked(const SessionSpec& spec, const FilterSpec& fs, uint64_t mask, const Entry* self,
                            std::string& err) const;

    SessionManagerConfig cfg_;
    SymbolTable& symbols_;
    Lvc& lvc_;
    std::vector<Partition*> partitions_;
    std::vector<Pipeline*> groups_;
    mutable std::mutex mutex_;
    uint64_t last_reader_service_ns_ = 0;
    std::vector<Entry> entries_;
    uint32_t next_id_ = 1;
};

} // namespace subplane
