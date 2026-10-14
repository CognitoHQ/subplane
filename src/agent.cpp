#include "subplane/agent.h"

#include "subplane/api_server.h"
#include "subplane/config_manager.h"
#include "subplane/core/clock.h"
#include "subplane/logging/log_registry.h"
#include "subplane/metrics/metrics_manager.h"
#include "subplane/pipeline/lvc.h"
#include "subplane/pipeline/partition.h"
#include "subplane/pipeline/pipeline.h"
#include "subplane/pipeline/session.h"
#include "subplane/pipeline/session_manager.h"
#include "subplane/symbol_table.h"
#include "subplane/control/control_server.h"

#include <iostream>
#include <unistd.h>

namespace subplane {

Agent::Agent() : running_(false) {
    config_manager_ = std::make_unique<ConfigManager>();
    metrics_manager_ = std::make_unique<MetricsManager>();
    api_server_ = std::make_unique<ApiServer>();
}

Agent::~Agent() {
    stop();
}

bool Agent::initialize(const std::string& config_path) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!config_manager_->load_config(config_path)) {
        std::cerr << "failed to load configuration from " << config_path << std::endl;
        return false;
    }
    const auto slash = config_path.find_last_of('/');
    config_dir_ = slash == std::string::npos ? "." : config_path.substr(0, slash);

    auto logging_node = config_manager_->get_section("logging");
    std::string logging_config = logging_node.IsDefined() ? YAML::Dump(logging_node) : "{}";
    if (!LogRegistry::instance().initialize(logging_config)) {
        std::cerr << "failed to initialize logging" << std::endl;
        return false;
    }
    LogRegistry::instance().process_started("subplane", std::to_string(getpid()));
    LogRegistry::instance().config_loaded(config_path);

    name_ = config_manager_->get_value<std::string>("agent.name", "subplane");
    version_ = config_manager_->get_value<std::string>("agent.version", "2.0.0-dev");
    max_symbols_ = config_manager_->get_value<uint32_t>("agent.max_symbols", 65536u);

    auto metrics_node = config_manager_->get_section("metrics");
    std::string metrics_config = metrics_node.IsDefined() ? YAML::Dump(metrics_node) : "{}";
    if (!metrics_manager_->initialize(metrics_config)) {
        LogRegistry::instance().error("AGENT", "INIT_FAILED", "metrics manager");
        return false;
    }
    metrics_manager_->set_provider([this](const std::string& format) { return render_metrics(format); });

    symbols_ = std::make_unique<SymbolTable>(max_symbols_);

    lvc_ = std::make_unique<Lvc>();
    {
        std::vector<uint8_t> types;
        for (uint8_t c = static_cast<uint8_t>(MsgType::TRADE); c <= static_cast<uint8_t>(MsgType::DEFINITION); ++c) types.push_back(c);
        const uint32_t lvc_payload = config_manager_->get_value<uint32_t>("lvc.max_payload", 512u);
        std::string err;
        if (!lvc_->init(max_symbols_, types, lvc_payload, err)) {
            LogRegistry::instance().error("AGENT", "INIT_FAILED", err);
            return false;
        }
    }

    std::string err;
    if (!build_partitions(err) || !build_groups(err)) {
        LogRegistry::instance().error("AGENT", "INIT_FAILED", err);
        std::cerr << err << std::endl;
        return false;
    }

    if (config_manager_->get_value<bool>("api.enabled", true)) {
        if (!api_server_->initialize(this)) {
            LogRegistry::instance().error("AGENT", "INIT_FAILED", "api server");
            return false;
        }
    }

    LogRegistry::instance().info("AGENT", "INITIALIZED",
        name_ + " partitions=" + std::to_string(partitions_.size()) + " groups=" + std::to_string(groups_.size()));
    return true;
}

bool Agent::build_partitions(std::string& err) {
    auto node = config_manager_->get_section("partitions");
    if (!node.IsDefined() || node.IsNull()) {
        LogRegistry::instance().warn("AGENT", "NO_PARTITIONS", "no partitions configured; agent will idle");
        return true;
    }
    if (!node.IsSequence()) {
        err = "partitions must be a sequence";
        return false;
    }
    uint16_t idx = 0;
    for (const auto& pn : node) {
        PartitionConfig cfg;
        if (!PartitionConfig::parse(pn, idx, config_dir_, cfg, err)) return false;
        for (const auto& existing : partitions_) {
            if (existing->name() == cfg.name) {
                err = "duplicate partition name " + cfg.name;
                return false;
            }
        }
        auto p = std::make_unique<Partition>();
        if (!p->build(cfg, err)) return false;
        for (const auto& w : p->decoder_warnings()) LogRegistry::instance().warn("PARTITION", cfg.name, w);
        partitions_.push_back(std::move(p));
        ++idx;
    }
    return true;
}

bool Agent::build_groups(std::string& err) {
    auto node = config_manager_->get_section("agent.thread_groups");
    std::vector<std::pair<std::string, std::vector<std::string>>> spec; // name -> partition names
    std::vector<int> cpus;
    if (node.IsDefined() && node.IsSequence() && node.size() > 0) {
        for (const auto& g : node) {
            std::string gname = g["name"].as<std::string>("g" + std::to_string(spec.size()));
            std::vector<std::string> parts;
            if (g["partitions"].IsDefined()) {
                for (const auto& p : g["partitions"]) parts.push_back(p.as<std::string>());
            }
            spec.emplace_back(gname, parts);
            cpus.push_back(g["cpu"].as<int>(-1));
        }
    } else {
        std::vector<std::string> all;
        for (const auto& p : partitions_) all.push_back(p->name());
        spec.emplace_back("g0", all);
        cpus.push_back(-1);
    }
    std::vector<bool> assigned(partitions_.size(), false);
    for (size_t gi = 0; gi < spec.size(); ++gi) {
        auto g = std::make_unique<Pipeline>(spec[gi].first, 0, *symbols_, *lvc_);
        for (const auto& pname : spec[gi].second) {
            bool found = false;
            for (size_t pi = 0; pi < partitions_.size(); ++pi) {
                if (partitions_[pi]->name() == pname) {
                    if (assigned[pi]) {
                        err = "partition " + pname + " assigned to more than one thread group";
                        return false;
                    }
                    assigned[pi] = true;
                    g->add_partition(partitions_[pi].get());
                    found = true;
                }
            }
            if (!found) {
                err = "thread group " + spec[gi].first + ": unknown partition " + pname;
                return false;
            }
        }
        groups_.push_back(std::move(g));
        group_cpus_.push_back(cpus[gi]);
    }
    for (size_t pi = 0; pi < partitions_.size(); ++pi) {
        if (!assigned[pi]) {
            err = "partition " + partitions_[pi]->name() + " is not in any thread group";
            return false;
        }
    }
    return true;
}

bool Agent::load_session_specs(std::vector<SessionSpec>& out, std::string& err) const {
    out.clear();
    auto node = config_manager_->get_section("sessions");
    if (!node.IsDefined() || node.IsNull()) return true;
    if (!node.IsSequence()) {
        err = "sessions must be a sequence";
        return false;
    }
    for (const auto& sn : node) {
        SessionSpec s;
        if (!SessionSpec::parse(sn, s, err)) return false;
        s.owner = "config";
        out.push_back(std::move(s));
    }
    return true;
}

bool Agent::start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_.load()) return false;

    epoch_ = now_ns();
    start_time_ = std::chrono::steady_clock::now();
    std::string err;

    // Pipelines were built with epoch 0; rebuild with the real epoch is not
    // needed because the epoch is only used for AGENT_RESTARTED events, which
    // are emitted after start. Recreate cheaply to keep one source of truth.
    {
        std::vector<std::unique_ptr<Pipeline>> fresh;
        for (auto& g : groups_) {
            auto ng = std::make_unique<Pipeline>(g->name(), epoch_, *symbols_, *lvc_);
            for (Partition* p : g->partitions()) ng->add_partition(p);
            fresh.push_back(std::move(ng));
        }
        groups_ = std::move(fresh);
    }

    for (auto& p : partitions_) {
        if (!p->open(err)) {
            LogRegistry::instance().error("AGENT", "START_FAILED", err);
            for (auto& q : partitions_) q->close();
            return false;
        }
        LogRegistry::instance().info("PARTITION", "OPEN", p->name() + " " + p->transport().describe());
    }

    for (size_t i = 0; i < groups_.size(); ++i) {
        if (!groups_[i]->start(group_cpus_[i], err)) {
            LogRegistry::instance().error("AGENT", "START_FAILED", err);
            return false;
        }
        if (!err.empty()) LogRegistry::instance().warn("AGENT", "PIN_FAILED", err);
        err.clear();
    }

    SessionManagerConfig smc;
    smc.ring_dir = config_manager_->get_value<std::string>("fanout.ring_dir", "/dev/shm");
    smc.agent_name = name_;
    smc.epoch = epoch_;
    smc.max_symbols = max_symbols_;
    {
        int linger_ms = config_manager_->get_value<int>("control.linger_ms", 5000);
        if (linger_ms < 0) linger_ms = 0;
        smc.linger_ns = static_cast<uint64_t>(linger_ms) * static_cast<uint64_t>(1'000'000);
    }
    std::vector<Partition*> praw;
    for (auto& p : partitions_) praw.push_back(p.get());
    std::vector<Pipeline*> graw;
    for (auto& g : groups_) graw.push_back(g.get());
    sessions_ = std::make_unique<SessionManager>(smc, *symbols_, *lvc_, praw, graw);

    std::vector<SessionSpec> specs;
    if (!load_session_specs(specs, err) || !sessions_->reconcile_config(specs, err)) {
        LogRegistry::instance().error("AGENT", "SESSIONS", err);
        // Not fatal: the agent runs; operators can fix and reload.
    }

    {
        ControlServer::Config cc;
        cc.socket_path = config_manager_->get_value<std::string>("control.socket_path", "/run/subplane/" + name_ + ".sock");
        cc.fallback_socket_path = config_manager_->get_value<std::string>("control.fallback_socket_path", "/tmp/subplane." + name_ + ".sock");
        cc.linger_ns = smc.linger_ns;
        control_ = std::make_unique<ControlServer>(cc, *sessions_, *symbols_, praw, name_, epoch_);
        if (!control_->start(err)) {
            LogRegistry::instance().error("AGENT", "CONTROL", err);
            control_.reset();
        } else {
            LogRegistry::instance().info("CONTROL", "LISTENING", control_->bound_path());
        }
    }

    if (!metrics_manager_->start()) {
        LogRegistry::instance().error("AGENT", "START_FAILED", "metrics manager");
        return false;
    }
    if (config_manager_->get_value<bool>("api.enabled", true)) {
        std::string host = config_manager_->get_value<std::string>("api.host", "127.0.0.1");
        int port = config_manager_->get_value<int>("api.port", 8080);
        if (!api_server_->start(host, static_cast<uint16_t>(port))) {
            LogRegistry::instance().error("AGENT", "START_FAILED", "api server");
            metrics_manager_->stop();
            return false;
        }
    }

    running_.store(true);
    LogRegistry::instance().info("AGENT", "STARTED", name_ + " epoch=" + std::to_string(epoch_));
    return true;
}

void Agent::stop() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_.load()) return;
    running_.store(false);
    if (api_server_) api_server_->stop();
    metrics_manager_->stop();
    if (control_) { control_->stop(); control_.reset(); }
    if (sessions_) sessions_->destroy_all();
    for (auto& g : groups_) g->stop();
    for (auto& p : partitions_) p->close();
    sessions_.reset();
    LogRegistry::instance().process_stopped("subplane", std::to_string(getpid()));
}

AgentStatus Agent::get_status() const {
    AgentStatus s;
    s.name = name_;
    s.version = version_;
    s.running = running_.load();
    s.epoch = epoch_;
    if (s.running) {
        s.uptime_seconds = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start_time_).count());
    }
    for (const auto& p : partitions_) {
        PartitionStatus ps;
        ps.name = p->name();
        ps.id = p->id();
        ps.state = partition_state_name(static_cast<PartitionState>(p->state_pub.load(std::memory_order_relaxed)));
        ps.last_sequence = p->last_seq_pub.load(std::memory_order_relaxed);
        const auto c = p->counters.snapshot();
        ps.packets = c.packets;
        ps.messages = c.messages;
        ps.unknown_templates = c.unknown_templates;
        ps.gaps = c.gaps;
        ps.last_rx_ns = p->last_rx_pub.load(std::memory_order_relaxed);
        s.partitions.push_back(std::move(ps));
    }
    if (sessions_) s.sessions = sessions_->status();
    return s;
}

bool Agent::reload_sessions(std::string& err) {
    if (!running_.load() || !sessions_) {
        err = "agent not running";
        return false;
    }
    if (!config_manager_->reload_config()) {
        err = "config reload failed";
        return false;
    }
    std::vector<SessionSpec> specs;
    if (!load_session_specs(specs, err)) return false;
    const bool ok = sessions_->reconcile_config(specs, err);
    LogRegistry::instance().config_reloaded(config_manager_->get_config_path());
    return ok;
}

bool Agent::lvc_read(const std::string& symbol, const std::string& msg_type, TickHeader& hdr,
                     std::vector<uint8_t>& payload, std::string& err) const {
    if (!symbols_ || !lvc_) {
        err = "not initialized";
        return false;
    }
    uint8_t code;
    if (!msg_type_from_name(msg_type, code)) {
        err = "unknown msg_type " + msg_type;
        return false;
    }
    uint32_t id = symbols_->lookup(symbol);
    if (id == 0) {
        char* end = nullptr;
        const unsigned long long v = strtoull(symbol.c_str(), &end, 10);
        if (end && *end == '\0' && !symbol.empty()) id = symbols_->lookup_u64(v);
    }
    if (id == 0) {
        err = "unknown symbol " + symbol;
        return false;
    }
    if (!lvc_->read(id, code, hdr, payload)) {
        err = "no value for " + symbol + "/" + msg_type;
        return false;
    }
    return true;
}

std::vector<std::pair<uint32_t, std::string>> Agent::symbols(uint32_t from_id, uint32_t max) const {
    std::vector<std::pair<uint32_t, std::string>> out;
    if (!symbols_) return out;
    const uint32_t n = symbols_->size();
    std::string name;
    for (uint32_t id = from_id == 0 ? 1 : from_id; id <= n && out.size() < max; ++id) {
        if (symbols_->resolve(id, name)) out.emplace_back(id, name);
    }
    return out;
}

std::string Agent::render_metrics(const std::string& format) const {
    const AgentStatus st = get_status();
    if (format == "text") {
        std::string o = "subplane_uptime_seconds " + std::to_string(st.uptime_seconds) + "\n";
        for (const auto& p : partitions_) {
            const auto c = p->counters.snapshot();
            const std::string l = "{partition=\"" + p->name() + "\"}";
            o += "subplane_partition_packets" + l + " " + std::to_string(c.packets) + "\n";
            o += "subplane_partition_messages" + l + " " + std::to_string(c.messages) + "\n";
            o += "subplane_partition_gaps" + l + " " + std::to_string(c.gaps) + "\n";
            o += "subplane_partition_unknown_templates" + l + " " + std::to_string(c.unknown_templates) + "\n";
            o += "subplane_partition_undecodable_tail" + l + " " + std::to_string(c.undecodable_tail) + "\n";
        }
        for (const auto& g : groups_) {
            const auto h = g->hist_recv_to_publish().snapshot();
            const std::string l = "{group=\"" + g->name() + "\"}";
            o += "subplane_recv_to_publish_ns_p50" + l + " " + std::to_string(percentile(h, 0.50)) + "\n";
            o += "subplane_recv_to_publish_ns_p99" + l + " " + std::to_string(percentile(h, 0.99)) + "\n";
            o += "subplane_recv_to_publish_ns_p999" + l + " " + std::to_string(percentile(h, 0.999)) + "\n";
            o += "subplane_recv_to_publish_count" + l + " " + std::to_string(h.count) + "\n";
        }
        return o;
    }
    std::string o = "{\"uptime_seconds\":" + std::to_string(st.uptime_seconds) + ",\"partitions\":{";
    bool first = true;
    for (const auto& p : partitions_) {
        o += (first ? "" : ",");
        first = false;
        o += "\"" + p->name() + "\":" + p->counters.to_json();
    }
    o += "},\"groups\":{";
    first = true;
    for (const auto& g : groups_) {
        o += (first ? "" : ",");
        first = false;
        o += "\"" + g->name() + "\":{\"loops\":" + std::to_string(g->loops()) + "," +
             to_json(g->hist_recv_to_publish().snapshot(), "recv_to_publish_ns") + "}";
    }
    o += "},\"sessions\":{";
    first = true;
    for (const auto& s : st.sessions) {
        o += (first ? "" : ",");
        first = false;
        o += "\"" + s.name + "\":{\"published\":" + std::to_string(s.published) + ",\"filtered\":" +
             std::to_string(s.filtered) + ",\"filter_generation\":" + std::to_string(s.filter_generation) + "}";
    }
    o += "}}";
    return o;
}

} // namespace subplane
