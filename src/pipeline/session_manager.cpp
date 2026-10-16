#include "subplane/pipeline/session_manager.h"

#include "subplane/core/clock.h"
#include "subplane/logging/log_registry.h"

#include <algorithm>
#include <unordered_set>

namespace subplane {

SessionManager::SessionManager(SessionManagerConfig cfg, SymbolTable& symbols, Lvc& lvc,
                               std::vector<Partition*> partitions, std::vector<Pipeline*> groups)
    : cfg_(std::move(cfg)), symbols_(symbols), lvc_(lvc),
      partitions_(std::move(partitions)), groups_(std::move(groups)) {}

SessionManager::~SessionManager() {
    destroy_all();
}

std::string SessionManager::ring_path(const std::string& name) const {
    return cfg_.ring_dir + "/subplane." + cfg_.agent_name + "." + name + ".ring";
}

SessionManager::Entry* SessionManager::find_locked(const std::string& name) {
    for (auto& e : entries_) {
        if (e.session && e.session->name == name) return &e;
    }
    return nullptr;
}

const SessionManager::Entry* SessionManager::find_locked(const std::string& name) const {
    for (const auto& e : entries_) {
        if (e.session && e.session->name == name) return &e;
    }
    return nullptr;
}

bool SessionManager::resolve(const SessionSpec& spec, FilterSpec& fs, uint64_t& partition_mask,
                             Pipeline*& group, std::string& err) const {
    fs = FilterSpec{};
    partition_mask = 0;
    group = nullptr;

    if (spec.partitions.empty()) {
        for (const Partition* p : partitions_) {
            partition_mask |= 1ull << p->id();
            fs.partition_ids.push_back(p->id());
        }
    } else {
        for (const auto& pname : spec.partitions) {
            const Partition* found = nullptr;
            for (const Partition* p : partitions_) {
                if (p->name() == pname) { found = p; break; }
            }
            if (!found) {
                err = "session " + spec.name + ": unknown partition " + pname;
                return false;
            }
            partition_mask |= 1ull << found->id();
            fs.partition_ids.push_back(found->id());
        }
    }
    if (partition_mask == 0) {
        err = "session " + spec.name + ": no partitions configured";
        return false;
    }
    for (Pipeline* g : groups_) {
        if ((g->partition_mask() & partition_mask) == partition_mask) { group = g; break; }
    }
    if (!group) {
        err = "session " + spec.name + ": its partitions span more than one thread group";
        return false;
    }

    // INTENT: a passthrough partition stamps the raw instrument id as symbol_id,
    // so a numeric filter entry must be used as-is, not interned. Mixing
    // passthrough and table partitions in one session would make the same
    // number mean two different things, so it is rejected.
    size_t n_pass = 0, n_part = 0, n_vscope = 0;
    for (const Partition* p : partitions_) {
        if (!((partition_mask >> p->id()) & 1ull)) continue;
        ++n_part;
        if (p->symbol_passthrough()) ++n_pass;
        if (p->symbol_venue_scope()) ++n_vscope;
    }
    if (n_pass != 0 && n_pass != n_part) {
        err = "session " + spec.name + ": mixes passthrough and table symbol partitions";
        return false;
    }
    const bool passthrough = n_pass != 0;

    fs.all_symbols = spec.all_symbols;
    for (const auto& sym : spec.symbols) {
        // INTENT: interning on the cold path is allowed (table is lock-free and
        // append-only); the pipeline thread may intern the same name
        // concurrently and will converge on the same id.
        // INTENT: the decoder interns integer instrument ids under the u64 key
        // space and char-array symbols under the string space. A digit-only
        // name is taken as an integer id first (lookup, then intern); only an
        // existing string entry of the same spelling overrides that.
        // "<venue>:<id>" names an id on a symbol_scope: venue partition; a bare
        // id there would intern under venue 0 and never match, so it is
        // rejected when every partition of the session is venue-scoped.
        uint32_t id = 0;
        uint64_t v = 0;
        uint16_t venue = 0;
        const bool numeric = parse_numeric_symbol(sym, v, venue);
        if (numeric && venue != 0 && (passthrough || n_vscope == 0)) {
            err = "session " + spec.name + ": symbol '" + sym + "' is venue-scoped but none of its partitions has symbol_scope: venue";
            return false;
        }
        if (numeric && venue == 0 && !passthrough && n_vscope == n_part) {
            err = "session " + spec.name + ": symbol '" + sym + "' needs a venue (<venue>:<id>) on symbol_scope: venue partitions";
            return false;
        }
        if (passthrough) {
            if (!numeric || v == 0 || v >= cfg_.max_symbols) {
                err = "session " + spec.name + ": symbol '" + sym + "' must be an instrument id in 1.." +
                      std::to_string(cfg_.max_symbols - 1) + " on a passthrough partition";
                return false;
            }
            id = static_cast<uint32_t>(v);
        } else if (numeric) {
            id = symbols_.lookup_u64(v, venue);
            if (id == 0) id = symbols_.lookup(sym);
            if (id == 0) id = symbols_.intern_u64(v, venue);
        } else {
            id = symbols_.intern(sym);
        }
        if (id == 0) {
            err = "session " + spec.name + ": cannot intern symbol " + sym + " (table full or invalid)";
            return false;
        }
        fs.symbol_ids.push_back(id);
    }
    if (spec.msg_types.empty()) {
        for (uint8_t c = 1; c <= MSG_TYPE_DATA_MAX && c < 64; ++c) fs.msg_types.push_back(c);
    } else {
        for (const auto& m : spec.msg_types) {
            uint8_t code;
            if (!msg_type_from_name(m, code)) {
                err = "session " + spec.name + ": unknown msg_type " + m;
                return false;
            }
            fs.msg_types.push_back(code);
        }
    }
    fs.bus_admin = spec.bus_admin;
    fs.agent_control = spec.agent_control;
    return true;
}

bool SessionManager::post_and_wait(Pipeline& g, const PipelineCommand& c, std::string& err) {
    const uint64_t start = now_mono_ns();
    while (!g.post(c)) {
        if (now_mono_ns() - start > cfg_.ack_timeout_ns) {
            err = "pipeline " + g.name() + ": command queue full";
            return false;
        }
        sched_yield();
    }
    if (g.running()) {
        if (!g.wait_applied(cfg_.ack_timeout_ns)) {
            err = "pipeline " + g.name() + ": command not acknowledged";
            return false;
        }
    } else {
        // Pipeline thread not running (startup or tests): apply inline.
        g.run_once();
    }
    return true;
}

bool SessionManager::create(const SessionSpec& spec, std::string& err) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (find_locked(spec.name)) {
        err = "session " + spec.name + " already exists";
        return false;
    }
    FilterSpec fs;
    uint64_t mask = 0;
    Pipeline* group = nullptr;
    if (!resolve(spec, fs, mask, group, err)) return false;
    if (!check_recap_locked(spec, fs, mask, nullptr, err)) return false;

    Entry e;
    e.spec = spec;
    e.group = group;
    e.generation = 1;
    e.session = std::make_unique<Session>();
    Session& s = *e.session;
    s.id = next_id_++;
    s.name = spec.name;
    s.owner = spec.owner;
    s.partition_mask = mask;
    s.recap = spec.recap;
    if (spec.recap) e.recap_symbols = fs.symbol_ids;
    s.filter_owned = build_filter(fs, cfg_.max_symbols, e.generation, err);
    if (!s.filter_owned) return false;
    s.filter.store(s.filter_owned.get(), std::memory_order_release);

    RingConfig rc;
    rc.capacity_bytes = spec.ring_capacity_bytes;
    rc.max_payload = spec.max_payload;
    rc.session_name = spec.name;
    const std::string path = ring_path(spec.name);
    RingWriter::unlink_ring(path); // stale ring from a previous incarnation
    if (!s.ring.open(rc, path, cfg_.epoch, err)) {
        err = "session " + spec.name + ": ring: " + err;
        return false;
    }
    s.ring.heartbeat(now_ns()); // liveness before the pipeline thread takes over the ring

    PipelineCommand c;
    c.type = PipelineCommand::Type::ADD_SESSION;
    c.session = &s;
    if (!post_and_wait(*group, c, err)) {
        s.ring.close();
        RingWriter::unlink_ring(path);
        return false;
    }
    e.client_connected = spec.owner == "client";
    entries_.push_back(std::move(e));
    LogRegistry::instance().info("SESSION", "CREATED", spec.name + " -> " + path);
    return true;
}

bool SessionManager::set_filter(const SessionSpec& spec, std::string& err) {
    std::lock_guard<std::mutex> lock(mutex_);
    Entry* e = find_locked(spec.name);
    if (!e) {
        err = "session " + spec.name + " not found";
        return false;
    }
    // INTENT: recap is fixed at creation; SET_FILTER keeps it whatever the
    // request says (its recap byte is defined for CREATE_SESSION only).
    SessionSpec want = spec;
    want.recap = e->spec.recap;
    FilterSpec fs;
    uint64_t mask = 0;
    Pipeline* group = nullptr;
    if (!resolve(want, fs, mask, group, err)) return false;
    if (mask != e->session->partition_mask) {
        err = "session " + spec.name + ": partitions cannot change after creation; destroy and recreate";
        return false;
    }
    if (!check_recap_locked(want, fs, mask, e, err)) return false;
    const uint32_t gen = e->generation + 1;
    auto fresh = build_filter(fs, cfg_.max_symbols, gen, err);
    if (!fresh) return false;

    PipelineCommand c;
    c.type = PipelineCommand::Type::SWAP_FILTER;
    c.session = e->session.get();
    c.filter = fresh.get();
    c.generation = gen;
    if (!post_and_wait(*e->group, c, err)) return false;
    // INTENT: the old table is freed only after the pipeline acked the swap;
    // see Session::filter.
    e->session->filter_retiring = std::move(e->session->filter_owned);
    e->session->filter_owned = std::move(fresh);
    e->session->filter_retiring.reset();
    e->generation = gen;
    if (want.recap) e->recap_symbols = fs.symbol_ids;
    SessionSpec keep = want;
    keep.owner = e->spec.owner;
    e->spec = keep;
    return true;
}

bool SessionManager::destroy_locked(Entry& e, uint32_t reason, std::string& err) {
    PipelineCommand c;
    c.type = PipelineCommand::Type::REMOVE_SESSION;
    c.session = e.session.get();
    c.generation = reason;
    const bool ok = post_and_wait(*e.group, c, err);
    e.session->ring.close();
    RingWriter::unlink_ring(e.session->ring.path());
    LogRegistry::instance().info("SESSION", "DESTROYED", e.session->name);
    return ok;
}

bool SessionManager::destroy(const std::string& name, uint32_t reason, std::string& err) {
    std::lock_guard<std::mutex> lock(mutex_);
    Entry* e = find_locked(name);
    if (!e) {
        err = "session " + name + " not found";
        return false;
    }
    const bool ok = destroy_locked(*e, reason, err);
    entries_.erase(std::remove_if(entries_.begin(), entries_.end(),
                                  [&](const Entry& x) { return x.session.get() == e->session.get() || !x.session; }),
                   entries_.end());
    return ok;
}

void SessionManager::destroy_all() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string err;
    for (auto& e : entries_) {
        if (e.session) destroy_locked(e, 0, err);
    }
    entries_.clear();
}

bool SessionManager::check_recap_locked(const SessionSpec& spec, const FilterSpec& fs, uint64_t mask, const Entry* self,
                                        std::string& err) const {
    if (!spec.recap) return true;
    if (spec.all_symbols) {
        err = "session " + spec.name + ": recap needs a symbol list, not all symbols";
        return false;
    }
    if (!spec.agent_control) {
        err = "session " + spec.name + ": recap needs agent control events (they mark where each snapshot starts and ends)";
        return false;
    }
    for (const Partition* p : partitions_) {
        if (!p->has_recap() || !((mask >> p->id()) & 1ull)) continue;
        std::unordered_set<uint32_t> distinct(fs.symbol_ids.begin(), fs.symbol_ids.end());
        for (const auto& e : entries_) {
            if (&e == self || !e.session || !e.session->recap || !((e.session->partition_mask >> p->id()) & 1ull)) continue;
            distinct.insert(e.recap_symbols.begin(), e.recap_symbols.end());
        }
        if (distinct.size() > p->recap_config().max_pending) {
            err = "session " + spec.name + ": partition " + p->name() + " would have " + std::to_string(distinct.size()) +
                  " recap symbols, more than its recap.max_pending (" + std::to_string(p->recap_config().max_pending) + ")";
            return false;
        }
    }
    return true;
}

bool SessionManager::specs_differ(const SessionSpec& a, const SessionSpec& b) const {
    return a.partitions != b.partitions || a.symbols != b.symbols || a.all_symbols != b.all_symbols ||
           a.msg_types != b.msg_types || a.bus_admin != b.bus_admin || a.agent_control != b.agent_control;
}

bool SessionManager::reconcile_config(const std::vector<SessionSpec>& specs, std::string& err) {
    std::vector<std::string> wanted;
    for (const auto& s : specs) wanted.push_back(s.name);

    // Destroy config-owned sessions that disappeared.
    std::vector<std::string> gone;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& e : entries_) {
            if (e.spec.owner == "config" && std::find(wanted.begin(), wanted.end(), e.session->name) == wanted.end()) {
                gone.push_back(e.session->name);
            }
        }
    }
    bool ok = true;
    for (const auto& n : gone) {
        std::string e2;
        if (!destroy(n, 2, e2)) { err += e2 + "; "; ok = false; }
    }
    for (const auto& s : specs) {
        SessionSpec spec = s;
        spec.owner = "config";
        bool exists = false, differs = false, client_owned = false, recap_changed = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (const Entry* e = find_locked(spec.name)) {
                exists = true;
                client_owned = e->spec.owner != "config";
                differs = specs_differ(e->spec, spec);
                recap_changed = e->spec.recap != spec.recap;
            }
        }
        std::string e2;
        if (!exists) {
            if (!create(spec, e2)) { err += e2 + "; "; ok = false; }
        } else if (client_owned) {
            err += "session " + spec.name + " is client-owned; config entry ignored; ";
            ok = false;
        } else if (recap_changed) {
            err += "session " + spec.name + ": recap cannot change after creation; destroy and recreate; ";
            ok = false;
        } else if (differs) {
            if (!set_filter(spec, e2)) { err += e2 + "; "; ok = false; }
        }
    }
    return ok;
}

bool SessionManager::attach_info(const std::string& name, SessionAttachInfo& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const Entry* e = find_locked(name);
    if (!e) return false;
    out.id = e->session->id;
    out.name = e->session->name;
    out.owner = e->session->owner;
    out.ring_path = e->session->ring.path();
    out.epoch = cfg_.epoch;
    out.capacity = e->session->ring.capacity();
    out.max_payload = e->session->ring.max_payload();
    out.filter_generation = e->generation;
    return true;
}

std::vector<SessionStatus> SessionManager::status() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<SessionStatus> out;
    for (const auto& e : entries_) {
        SessionStatus s;
        s.name = e.session->name;
        s.id = e.session->id;
        s.owner = e.session->owner;
        s.ring_path = e.session->ring.path();
        s.epoch = cfg_.epoch;
        const auto c = e.session->counters.snapshot();
        s.published = c.published;
        s.filtered = c.filtered;
        s.publish_failed = c.publish_failed;
        s.ring_overruns = c.ring_overruns;
        s.filter_generation = e.generation;
        s.ring_head = e.session->ring.published_head();
        s.ring_capacity = e.session->ring.capacity();
        s.heartbeat_ns = e.session->ring.heartbeat_ns();
        out.push_back(std::move(s));
    }
    return out;
}

size_t SessionManager::count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return entries_.size();
}

void SessionManager::client_disconnected(const std::string& name, uint64_t now_ns_) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (Entry* e = find_locked(name); e && e->spec.owner == "client") {
        e->client_connected = false;
        e->disconnect_ns = now_ns_;
    }
}

void SessionManager::client_reattached(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (Entry* e = find_locked(name); e && e->spec.owner == "client") {
        e->client_connected = true;
        e->disconnect_ns = 0;
    }
}

void SessionManager::service_linger(uint64_t now_ns_) {
    std::vector<std::string> expired;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& e : entries_) {
            if (e.spec.owner == "client" && !e.client_connected && e.disconnect_ns != 0 &&
                now_ns_ - e.disconnect_ns > cfg_.linger_ns) {
                expired.push_back(e.session->name);
            }
        }
    }
    for (const auto& n : expired) {
        std::string err;
        destroy(n, 1, err);
        LogRegistry::instance().info("SESSION", "LINGER_EXPIRED", n);
    }
}

bool SessionManager::snapshot(const std::string& name, uint64_t& ring_head_at_snapshot, const SnapshotFn& fn) const {
    const FilterTable* f = nullptr;
    uint64_t head = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const Entry* e = find_locked(name);
        if (!e) return false;
        f = e->session->filter.load(std::memory_order_acquire);
        head = e->session->ring.published_head();
    }
    ring_head_at_snapshot = head;
    if (!f) return true;
    // INTENT: iterate the LVC outside the manager lock; the filter table is
    // immutable and only freed after a swap ack, and a concurrent swap during a
    // snapshot only means we snapshot the previous filter, which the client
    // reconciles by sequence anyway.
    TickHeader h;
    std::vector<uint8_t> payload;
    const uint32_t max_sym = lvc_.max_symbols();
    for (uint8_t t : lvc_.tracked_types()) {
        if (t >= 64 || !((f->msg_type_mask >> t) & 1u)) continue;
        for (uint32_t sid = 1; sid < max_sym; ++sid) {
            if (!f->all_symbols) {
                const uint32_t w = sid >> 6;
                if (w >= f->symbol_bits.size() || !((f->symbol_bits[w] >> (sid & 63u)) & 1u)) continue;
            }
            if (!lvc_.read(sid, t, h, payload)) continue;
            if (!f->accept(h)) continue;
            h.flags = static_cast<uint8_t>(h.flags | FLAG_SNAPSHOT_ORIGIN);
            fn(h, payload.data(), static_cast<uint32_t>(payload.size()));
        }
    }
    return true;
}

} // namespace subplane
