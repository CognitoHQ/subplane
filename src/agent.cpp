#include "subplane/agent.h"

#include "subplane/api_server.h"
#include "subplane/config_manager.h"
#include "subplane/core/clock.h"
#include "subplane/logging/log_registry.h"
#include "subplane/metrics/metrics_manager.h"
#include "subplane/metrics/prom_writer.h"
#include "subplane/observability/endpoint_key.h"
#include "subplane/observability/proc_scan.h"
#include "subplane/pipeline/lvc.h"
#include "subplane/pipeline/partition.h"
#include "subplane/pipeline/pipeline.h"
#include "subplane/recap/recap_manager.h"
#include "subplane/pipeline/session.h"
#include "subplane/pipeline/session_manager.h"
#include "subplane/symbol_table.h"
#include "subplane/control/control_server.h"

#include <cstdio>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
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

    {
        char host[256] = {};
        host_ = ::gethostname(host, sizeof host - 1) == 0 ? host : "unknown";
        site_ = config_manager_->get_value<std::string>("observability.site", "");
        env_ = config_manager_->get_value<std::string>("observability.env", "");
        auto labels = config_manager_->get_section("observability");
        if (labels.IsMap() && labels["labels"].IsMap()) {
            for (const auto& kv : labels["labels"]) {
                const std::string k = kv.first.as<std::string>();
                bool ok = !k.empty() && !(k[0] >= '0' && k[0] <= '9') && k.rfind("__", 0) != 0;
                for (char c : k) ok = ok && ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_');
                if (!ok) {
                    std::cerr << "observability.labels: invalid label name '" << k << "'" << std::endl;
                    return false;
                }
                info_labels_.emplace_back(k, kv.second.as<std::string>());
            }
        }
    }

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

    // INTENT: recap components open before the pipelines start, so a bad
    // recap block fails the start like a bad transport does. A snapshot
    // service that is merely down is not an error: requests fail and retry.
    recap_.reset();
    for (size_t gi = 0; gi < groups_.size(); ++gi) {
        for (Partition* p : groups_[gi]->partitions()) {
            if (!p->has_recap()) continue;
            if (!recap_) recap_ = std::make_unique<RecapManager>(epoch_, *symbols_);
            // Before the manager takes the component: /topology needs its key.
            if (const RecapTransport* rt = p->recap_transport()) recap_keys_[p->id()] = rt->endpoint_key(host_);
            if (!recap_->add_partition(*p, groups_[gi]->recap_channel(), err)) {
                LogRegistry::instance().error("AGENT", "START_FAILED", err);
                for (auto& q : partitions_) q->close();
                return false;
            }
        }
    }
    if (recap_ && !recap_->open(err)) {
        LogRegistry::instance().error("AGENT", "START_FAILED", err);
        recap_.reset();
        for (auto& q : partitions_) q->close();
        return false;
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

    if (recap_) recap_->start();

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
    if (recap_) { recap_->stop(); recap_.reset(); }
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
        ps.transport = p->has_transport() ? transport_state_name(p->transport().state()) : "unknown";
        ps.transport_down = c.transport_down;
        ps.transport_up = c.transport_up;
        if (p->has_recap()) ps.recap_json = p->recap_counters.to_json();
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
        uint64_t v = 0;
        uint16_t venue = 0;
        if (parse_numeric_symbol(symbol, v, venue)) id = symbols_->lookup_u64(v, venue);
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

namespace {
std::string jstr(std::string_view s) {
    std::string o = "\"";
    for (char c : s) {
        if (c == '"' || c == '\\') { o += '\\'; o += c; }
        else if (static_cast<unsigned char>(c) < 0x20) { char b[8]; std::snprintf(b, sizeof b, "\\u%04x", c); o += b; }
        else o += c;
    }
    return o + "\"";
}
} // namespace

Agent::ConsumerScan Agent::consumers(const std::vector<SessionStatus>& sessions) const {
    std::lock_guard<std::mutex> lock(scan_mutex_);
    const uint64_t now = now_mono_ns();
    if (scan_at_ns_ != 0 && now - scan_at_ns_ < kScanCacheNs) return scan_cache_;
    std::unordered_set<std::string> rings;
    std::unordered_map<std::string, std::string> session_of; // ring path -> session
    for (const auto& s : sessions) { rings.insert(s.ring_path); session_of[s.ring_path] = s.name; }
    const observability::ScanResult r = observability::scan_mappers(rings, static_cast<int32_t>(::getpid()));
    ConsumerScan out;
    out.processes = r.processes;
    out.unreadable = r.unreadable;
    for (const auto& m : r.mappers) {
        Consumer c;
        c.session = session_of[m.path];
        c.pid = m.pid;
        c.start_time = m.start_time;
        c.uid = m.uid;
        c.comm = m.comm;
        c.unit = m.unit;
        c.mapping = true;
        c.stale = m.deleted;
        out.consumers.push_back(std::move(c));
    }
    // INTENT: a control client is the same process as a mapper when the pid
    // matches; its IDENTIFY name then labels that entry. A client that holds
    // a session but has not mapped the ring (yet) is listed on its own.
    if (control_) {
        for (const auto& cl : control_->client_snapshot()) {
            for (const auto& sname : cl.sessions) {
                bool merged = false;
                for (auto& c : out.consumers) {
                    if (c.session == sname && c.pid == cl.pid && !c.stale) { c.control = true; c.name = cl.app_name; merged = true; }
                }
                if (merged) continue;
                Consumer c;
                observability::Mapper id;
                if (observability::read_identity("/proc", cl.pid, id)) { c.start_time = id.start_time; c.comm = id.comm; c.unit = id.unit; }
                c.session = sname;
                c.pid = cl.pid;
                c.uid = cl.uid;
                c.name = cl.app_name;
                c.control = true;
                out.consumers.push_back(std::move(c));
            }
        }
    }
    scan_cache_ = out;
    scan_at_ns_ = now;
    return out;
}

std::string Agent::render_topology() const {
    const AgentStatus st = get_status();
    std::string o = "{\"schema\":\"subplane.topology/1\",\"node\":{";
    o += "\"id\":" + jstr(host_ + "/subplaned/" + name_);
    o += ",\"service\":\"subplaned\",\"instance\":" + jstr(name_) + ",\"host\":" + jstr(host_);
    o += ",\"pid\":" + std::to_string(::getpid()) + ",\"version\":" + jstr(version_) + ",\"epoch\":" + std::to_string(epoch_);
    o += ",\"site\":" + jstr(site_) + ",\"env\":" + jstr(env_) + "},\"upstream\":[";
    bool first = true;
    for (size_t i = 0; i < partitions_.size() && i < st.partitions.size(); ++i) {
        const Partition& p = *partitions_[i];
        const PartitionStatus& ps = st.partitions[i];
        if (p.has_transport()) {
            o += first ? "" : ",";
            first = false;
            o += "{\"key\":" + jstr(p.transport().endpoint_key(host_)) + ",\"role\":\"data\",\"state\":" + jstr(ps.transport);
            o += ",\"local\":{\"partition\":" + jstr(p.name()) + "},\"describe\":" + jstr(p.transport().describe());
            const std::string pub = p.transport().publisher_json();
            if (!pub.empty()) o += ",\"publisher\":{" + pub + "}";
            o += "}";
        }
        if (p.has_recap()) {
            o += first ? "" : ",";
            first = false;
            const bool up = p.recap_counters.connected.load(std::memory_order_relaxed) != 0;
            o += "{\"key\":" + jstr(recap_keys_[p.id()]) + ",\"role\":\"recap\",\"state\":" + jstr(up ? "up" : "down");
            o += ",\"local\":{\"partition\":" + jstr(p.name()) + "}}";
        }
    }
    o += "],\"downstream\":[";
    const ConsumerScan scan = consumers(st.sessions);
    first = true;
    for (const auto& s : st.sessions) {
        o += first ? "" : ",";
        first = false;
        o += "{\"key\":" + jstr(observability::shm_ring_key(s.ring_path, host_)) + ",\"role\":\"data\",\"state\":\"up\"";
        o += ",\"local\":{\"session\":" + jstr(s.name) + ",\"owner\":" + jstr(s.owner) + ",\"partitions\":[";
        for (size_t k = 0; k < s.partitions.size(); ++k) o += (k ? "," : "") + jstr(s.partitions[k]);
        o += "]},\"consumers\":[";
        bool fc = true;
        for (const auto& c : scan.consumers) {
            if (c.session != s.name) continue;
            o += fc ? "" : ",";
            fc = false;
            o += "{\"pid\":" + std::to_string(c.pid) + ",\"start_time\":" + std::to_string(c.start_time) + ",\"uid\":" + std::to_string(c.uid);
            o += ",\"comm\":" + jstr(c.comm) + ",\"name\":" + jstr(c.name.empty() ? (c.unit.empty() ? c.comm : c.unit) : c.name);
            o += ",\"unit\":" + jstr(c.unit);
            if (c.stale) o += ",\"stale_ring\":true";
            o += ",\"via\":[";
            o += c.control ? "\"control\"" : "";
            o += c.mapping ? (c.control ? ",\"mapping\"" : "\"mapping\"") : "";
            o += "]}";
        }
        o += "]}";
    }
    o += "],\"scan\":{\"processes\":" + std::to_string(scan.processes) + ",\"unreadable\":" + std::to_string(scan.unreadable) + "}}";
    return o;
}

std::string Agent::render_metrics(const std::string& format) const {
    const AgentStatus st = get_status();
    if (format == "json") {
        std::string o = "{\"uptime_seconds\":" + std::to_string(st.uptime_seconds) + ",\"partitions\":{";
        bool first = true;
        for (const auto& p : partitions_) {
            o += (first ? "" : ",");
            first = false;
            o += "\"" + p->name() + "\":" + p->counters.to_json();
        }
        o += "},\"recap\":{";
        first = true;
        for (const auto& p : partitions_) {
            if (!p->has_recap()) continue;
            o += (first ? "" : ",");
            first = false;
            o += "\"" + p->name() + "\":" + p->recap_counters.to_json();
        }
        o += "},\"groups\":{";
        first = true;
        for (const auto& g : groups_) {
            o += (first ? "" : ",");
            first = false;
            o += "\"" + g->name() + "\":{\"loops\":" + std::to_string(g->loops()) + ",\"recv_to_publish_ns\":" +
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

    // Prometheus text exposition (docs/context/observability.md): every
    // format other than "json", including the old "text".
    PromWriter w({{"service", "subplaned"}, {"instance", name_}, {"host", host_}});
    const uint64_t now = now_ns();
    auto age_s = [now](uint64_t t) { return t == 0 || t > now ? 0.0 : static_cast<double>(now - t) / 1e9; };

    PromWriter::Labels info = {{"version", version_}, {"epoch", std::to_string(epoch_)}, {"site", site_}, {"env", env_}};
    info.insert(info.end(), info_labels_.begin(), info_labels_.end());
    w.gauge("subplane_service_info", "Identity of this service; always 1", info, 1);
    w.gauge("subplane_uptime_seconds", "Seconds since the agent started", {}, static_cast<double>(st.uptime_seconds));
    w.gauge("subplane_sessions", "Sessions open on this agent", {}, static_cast<double>(st.sessions.size()));
    if (control_) w.gauge("subplane_control_clients", "Clients connected to the control socket", {}, static_cast<double>(control_->clients()));
    if (symbols_) {
        w.gauge("subplane_symbols", "Symbols interned in the symbol table", {}, symbols_->size());
        w.gauge("subplane_symbols_capacity", "Symbol table capacity (agent.max_symbols)", {}, symbols_->capacity());
    }
    if (lvc_) w.counter("subplane_lvc_writes_total", "Last-value cache slot writes", {}, lvc_->writes());

    for (size_t i = 0; i < partitions_.size() && i < st.partitions.size(); ++i) {
        const Partition& p = *partitions_[i];
        const PartitionStatus& ps = st.partitions[i];
        const PromWriter::Labels l = {{"partition", p.name()}};
        PartitionCounters::each(p.counters.snapshot(), [&](const char* n, uint64_t v) {
            w.counter("subplane_partition_" + std::string(n) + "_total", "Partition counter " + std::string(n), l, v);
        });
        for (const char* s : {"init", "active", "degraded"}) {
            w.gauge("subplane_partition_state", "1 for the partition's current state", {{"partition", p.name()}, {"state", s}}, ps.state == s ? 1 : 0);
        }
        w.gauge("subplane_partition_last_sequence", "Last sequence accepted", l, static_cast<double>(ps.last_sequence));
        w.gauge("subplane_partition_last_receive_age_seconds", "Seconds since the last packet (0 before the first)", l, age_s(ps.last_rx_ns));
        for (const char* s : {"unknown", "up", "down"}) {
            w.gauge("subplane_transport_state", "1 for the transport's current state", {{"partition", p.name()}, {"state", s}}, ps.transport == s ? 1 : 0);
        }
        if (p.has_transport()) {
            p.transport().counters([&](std::string_view n, uint64_t v) {
                w.counter("subplane_transport_" + std::string(n) + "_total", "Transport counter " + std::string(n), l, v);
            });
        }
        if (p.has_recap()) {
            p.recap_counters.each([&](const char* n, uint64_t v) {
                std::string_view sn(n);
                if (sn.rfind("recap_", 0) == 0) sn.remove_prefix(6);
                w.counter("subplane_recap_" + std::string(sn) + "_total", "Recap counter " + std::string(n), l, v);
            });
            w.gauge("subplane_recap_inflight", "Recap requests in flight", l, static_cast<double>(p.recap_counters.inflight.load(std::memory_order_relaxed)));
            w.gauge("subplane_recap_queued", "Recap requests waiting to be sent", l, static_cast<double>(p.recap_counters.queued.load(std::memory_order_relaxed)));
            w.gauge("subplane_recap_connected", "1 while the recap component is connected to its snapshot service", l, static_cast<double>(p.recap_counters.connected.load(std::memory_order_relaxed)));
        }
    }
    for (const auto& g : groups_) {
        const PromWriter::Labels l = {{"group", g->name()}};
        w.counter("subplane_group_loops_total", "Pipeline loop iterations", l, g->loops());
        w.histogram_ns("subplane_recv_to_publish_seconds", "Packet receive timestamp to after its last publish", l, g->hist_recv_to_publish().snapshot());
    }
    for (const auto& s : st.sessions) {
        const PromWriter::Labels l = {{"session", s.name}};
        w.counter("subplane_session_published_total", "Records published to the session's ring", l, s.published);
        w.counter("subplane_session_filtered_total", "Records the session's filter rejected", l, s.filtered);
        w.counter("subplane_session_publish_failed_total", "Records that did not fit the ring", l, s.publish_failed);
        w.counter("subplane_session_ring_bytes_total", "Bytes written to the session's ring", l, s.ring_head);
        w.gauge("subplane_session_ring_capacity_bytes", "Ring data capacity", l, static_cast<double>(s.ring_capacity));
        w.gauge("subplane_session_ring_heartbeat_age_seconds", "Seconds since the writer's last ring heartbeat", l, age_s(s.heartbeat_ns));
        w.gauge("subplane_session_filter_generation", "Generation of the session's current filter", l, s.filter_generation);
    }

    // Topology as metrics: the same edges as /topology, so PromQL and alerts
    // can join them without the collector. 1 = up, 0 = not up.
    for (size_t i = 0; i < partitions_.size() && i < st.partitions.size(); ++i) {
        const Partition& p = *partitions_[i];
        if (p.has_transport()) {
            w.gauge("subplane_topology_edge", "An upstream or downstream endpoint: 1 up, 0 not up",
                    {{"direction", "upstream"}, {"role", "data"}, {"key", p.transport().endpoint_key(host_)}, {"partition", p.name()}},
                    st.partitions[i].transport == "up" ? 1 : 0);
        }
        if (p.has_recap()) {
            w.gauge("subplane_topology_edge", "An upstream or downstream endpoint: 1 up, 0 not up",
                    {{"direction", "upstream"}, {"role", "recap"}, {"key", recap_keys_[p.id()]}, {"partition", p.name()}},
                    p.recap_counters.connected.load(std::memory_order_relaxed) != 0 ? 1 : 0);
        }
    }
    const ConsumerScan scan = consumers(st.sessions);
    for (const auto& s : st.sessions) {
        w.gauge("subplane_topology_edge", "An upstream or downstream endpoint: 1 up, 0 not up",
                {{"direction", "downstream"}, {"role", "data"}, {"key", observability::shm_ring_key(s.ring_path, host_)}, {"session", s.name}}, 1);
        double n = 0;
        for (const auto& c : scan.consumers) n += c.session == s.name && !c.stale ? 1 : 0;
        w.gauge("subplane_session_consumers", "Processes reading the session's ring (control clients and ring mappers)", {{"session", s.name}}, n);
    }
    w.gauge("subplane_topology_scan_unreadable", "Processes whose memory maps the consumer scan could not read", {}, scan.unreadable);
    return w.str();
}

} // namespace subplane
