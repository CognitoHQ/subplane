#pragma once

// Agent orchestrator: owns config, logging, metrics, the HTTP API, the symbol
// table, the last-value cache, partitions (activations), pipeline thread
// groups, the session manager and the control channel.
// Thread-safety: initialize/start/stop are serialized by mutex_; get_status,
// get_metrics_manager, reload_sessions, lvc_read and symbols are safe from any
// thread.

#include "subplane/status_types.h"
#include "subplane/core/message.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace subplane {

class ConfigManager;
class MetricsManager;
class ApiServer;
class SymbolTable;
class Lvc;
class Partition;
class Pipeline;
class SessionManager;
class ControlServer;
class RecapManager;

class Agent {
public:
    Agent();
    ~Agent();

    bool initialize(const std::string& config_path);
    bool start();
    void stop();
    bool is_running() const { return running_.load(); }

    ConfigManager&  get_config_manager()  { return *config_manager_; }
    MetricsManager& get_metrics_manager() { return *metrics_manager_; }
    ApiServer*      get_api_server()      { return api_server_.get(); }

    // Snapshot of operator-visible state. Safe from any thread.
    AgentStatus get_status() const;

    // Re-reads the `sessions` section of the config and applies it. Safe from any thread.
    bool reload_sessions(std::string& err);

    // Last-value cache lookup by symbol name (or numeric id string) and msg type name.
    bool lvc_read(const std::string& symbol, const std::string& msg_type, TickHeader& hdr,
                  std::vector<uint8_t>& payload, std::string& err) const;

    // Interned symbols [from_id, from_id+max). Safe from any thread.
    std::vector<std::pair<uint32_t, std::string>> symbols(uint32_t from_id, uint32_t max) const;

    // Latency tracing for a session's readers (all of them, or one pid's
    // slots); docs/context/observability.md, "Reader slots". Any thread.
    bool set_reader_trace(const std::string& session, bool on, int32_t pid, uint32_t sample_every, uint32_t& matched,
                          std::string& err);

    // Writer incarnation, set at start(). 0 before start.
    uint64_t epoch() const { return epoch_; }

private:
    bool build_partitions(std::string& err);
    bool build_groups(std::string& err);
    bool load_session_specs(std::vector<struct SessionSpec>& out, std::string& err) const;
    std::string render_metrics(const std::string& format) const;
public:
    // Cold. The /topology document (docs/context/observability.md).
    std::string render_topology() const;
private:
    // The apps reading a session's ring: control-socket clients that created
    // or attached to it, and processes mapping its ring file (/proc scan,
    // cached for kScanCacheNs). One entry per (session, process).
    struct Consumer {
        std::string session;
        int32_t     pid = 0;
        uint64_t    start_time = 0;
        uint32_t    uid = 0;
        std::string comm, name, unit;
        bool        control = false, mapping = false, slot = false;
        bool        stale = false;      // maps an unlinked ring from an earlier agent run
    };
    struct ConsumerScan {
        std::vector<Consumer> consumers;
        uint32_t processes = 0, unreadable = 0;
    };
    ConsumerScan consumers(const std::vector<SessionStatus>& sessions) const;
    static constexpr uint64_t kScanCacheNs = 2'000'000'000ull;
    mutable std::mutex   scan_mutex_;
    mutable uint64_t     scan_at_ns_ = 0;
    mutable ConsumerScan scan_cache_;

    std::unique_ptr<ConfigManager>  config_manager_;
    std::unique_ptr<MetricsManager> metrics_manager_;
    std::unique_ptr<ApiServer>      api_server_;
    std::unique_ptr<SymbolTable>    symbols_;
    std::unique_ptr<Lvc>            lvc_;
    std::vector<std::unique_ptr<Partition>> partitions_;
    std::vector<std::unique_ptr<Pipeline>>  groups_;
    std::vector<int>                group_cpus_;
    std::unique_ptr<SessionManager> sessions_;
    std::unique_ptr<ControlServer>  control_;
    std::unique_ptr<RecapManager>   recap_;

    std::atomic<bool> running_;
    uint64_t epoch_ = 0;
    std::chrono::steady_clock::time_point start_time_;
    std::string name_;
    std::string version_;
    std::string config_dir_;
    uint32_t    max_symbols_ = 65536;
    // `observability:` identity (docs/context/observability.md).
    std::string host_;
    std::string site_, env_;
    std::vector<std::pair<std::string, std::string>> info_labels_;
    std::string recap_keys_[64];            // by partition id: the recap request stream's endpoint key
    mutable std::mutex mutex_;
};

} // namespace subplane
