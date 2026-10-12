#pragma once

// Agent orchestrator: owns config, logging, metrics, the HTTP API, the symbol
// table, the last-value cache, partitions (activations), pipeline thread
// groups, the session manager and the control channel.
// Thread-safety: initialize/start/stop are serialized by mutex_; get_status,
// get_metrics_manager, reload_sessions, lvc_read and symbols are safe from any
// thread.

#include "flashtick/status_types.h"
#include "flashtick/core/message.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace flashtick {

class ConfigManager;
class MetricsManager;
class ApiServer;
class SymbolTable;
class Lvc;
class Partition;
class Pipeline;
class SessionManager;
class ControlServer;

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

    // Writer incarnation, set at start(). 0 before start.
    uint64_t epoch() const { return epoch_; }

private:
    bool build_partitions(std::string& err);
    bool build_groups(std::string& err);
    bool load_session_specs(std::vector<struct SessionSpec>& out, std::string& err) const;
    std::string render_metrics(const std::string& format) const;

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

    std::atomic<bool> running_;
    uint64_t epoch_ = 0;
    std::chrono::steady_clock::time_point start_time_;
    std::string name_;
    std::string version_;
    std::string config_dir_;
    uint32_t    max_symbols_ = 65536;
    mutable std::mutex mutex_;
};

} // namespace flashtick
