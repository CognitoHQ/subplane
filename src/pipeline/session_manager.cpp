#include "subplane/pipeline/session_manager.h"
#include "subplane/pipeline/topic_registry.h"

#include "subplane/core/clock.h"
#include "subplane/logging/log_registry.h"
#include "subplane/observability/proc_scan.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <csignal>
#include <unordered_set>

namespace subplane {

SessionManager::SessionManager(SessionManagerConfig cfg, SymbolTable& symbols, Lvc& lvc,
                               std::vector<Partition*> partitions, std::vector<Pipeline*> groups,
                               std::vector<TopicDirectory*> directories)
    : cfg_(std::move(cfg)), symbols_(symbols), lvc_(lvc),
      partitions_(std::move(partitions)), groups_(std::move(groups)), directories_(std::move(directories)) {}

uint64_t SessionManager::names_version() const noexcept {
    uint64_t v = 0;
    for (const TopicDirectory* d : directories_) v += d->version();
    return v;
}

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
                             Pipeline*& group, std::string& err, std::vector<UnresolvedName>* unresolved) const {
    if (unresolved) unresolved->clear();
    fs = FilterSpec{};
    partition_mask = 0;
    group = nullptr;

    // Topic patterns resolve here, on the cold path, to the partitions the
    // session is attached to and the topic ids its filter accepts.
    for (const auto& pat : spec.topics) {
        bool any = false;
        for (const Partition* p : partitions_) {
            if (topic_matches(pat, p->topic_name())) { any = true; break; }
        }
        if (!any) {
            err = "session " + spec.name + ": topic pattern " + pat + " matches no topic";
            return false;
        }
    }
    for (const Partition* p : partitions_) {
        bool want = spec.topics.empty();
        for (const auto& pat : spec.topics) {
            if (topic_matches(pat, p->topic_name())) { want = true; break; }
        }
        if (!want) continue;
        partition_mask |= 1ull << p->id();
        if (std::find(fs.topic_ids.begin(), fs.topic_ids.end(), p->topic_id()) == fs.topic_ids.end())
            fs.topic_ids.push_back(p->topic_id());
    }
    if (partition_mask == 0) {
        err = "session " + spec.name + ": no topics configured";
        return false;
    }
    for (Pipeline* g : groups_) {
        if ((g->partition_mask() & partition_mask) == partition_mask) { group = g; break; }
    }
    if (!group) {
        err = "session " + spec.name + ": its topics span more than one thread group";
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
        err = "session " + spec.name + ": mixes topics with passthrough and table symbol ids";
        return false;
    }
    const bool passthrough = n_pass != 0;

    // The session's topics that have a resolver: on them a non-numeric symbol
    // is a ticker name, looked up in the topic's list.
    std::vector<const TopicDirectory*> named;
    for (const TopicDirectory* d : directories_) {
        if (std::find(fs.topic_ids.begin(), fs.topic_ids.end(), d->topic_id()) != fs.topic_ids.end()) named.push_back(d);
    }
    auto topic_partition = [&](uint16_t topic_id) -> const Partition* {
        for (const Partition* p : partitions_) if (p->topic_id() == topic_id) return p;
        return nullptr;
    };

    fs.all_symbols = spec.all_symbols;
    for (const auto& sym : spec.symbols) {
        uint64_t nv = 0;
        uint16_t nvenue = 0;
        if (!named.empty() && !parse_numeric_symbol(sym, nv, nvenue)) {
            // INTENT: a name must match in exactly one of the session's topics
            // (Satya, 2026-11-13); `<topic>:<name>` picks the topic. The lists
            // arrive after the resolvers connect, so a miss is not an error
            // here: the name is left out and resolved again when a list changes.
            std::string_view name = sym;
            const TopicDirectory* only = nullptr;
            if (const size_t colon = name.find(':'); colon != std::string_view::npos) {
                for (const TopicDirectory* d : named) {
                    if (d->topic_name() == name.substr(0, colon)) { only = d; name.remove_prefix(colon + 1); break; }
                }
            }
            UnresolvedName miss;
            miss.name = sym;
            miss.topic_id = only ? only->topic_id() : 0;
            const TopicDirectory* hit_dir = nullptr;
            ResolvedInstrument hit;
            uint32_t hits = 0;
            bool all_listed = true;
            for (const TopicDirectory* d : named) {
                if (only && d != only) continue;
                all_listed = all_listed && d->version() != 0;
                ResolvedInstrument r;
                if (d->find(name, r)) { ++hits; hit = r; hit_dir = d; }
            }
            if (hits != 1) {
                miss.reason = hits > 1 ? UnresolvedReason::AMBIGUOUS : UnresolvedReason::UNKNOWN;
                miss.listed = hits > 1 || all_listed;
                if (unresolved) unresolved->push_back(miss);
                continue;
            }
            const Partition* p = topic_partition(hit_dir->topic_id());
            uint32_t id = 0;
            if (p != nullptr && p->symbol_passthrough()) id = hit.instrument < cfg_.max_symbols ? static_cast<uint32_t>(hit.instrument) : 0;
            else if (p != nullptr) id = symbols_.intern_u64(hit.instrument, p->symbol_venue_scope() ? hit.venue : uint16_t{0});
            if (id == 0) {
                err = "session " + spec.name + ": cannot intern symbol " + sym + " (table full or id out of range)";
                return false;
            }
            fs.symbol_ids.push_back(id);
            continue;
        }
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
    std::vector<UnresolvedName> unresolved;
    const uint64_t version = names_version();
    if (!resolve(spec, fs, mask, group, err, &unresolved)) return false;
    if (!check_recap_locked(spec, fs, mask, nullptr, err)) return false;

    Entry e;
    e.unresolved = std::move(unresolved);
    e.names_version = version;
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
    rc.mode = cfg_.ring_mode;
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
    report_unresolved_locked(entries_.back());
    return true;
}

bool SessionManager::set_filter(const SessionSpec& spec, std::string& err) {
    std::lock_guard<std::mutex> lock(mutex_);
    Entry* e = find_locked(spec.name);
    if (!e) {
        err = "session " + spec.name + " not found";
        return false;
    }
    return set_filter_locked(*e, spec, err);
}

bool SessionManager::set_filter_locked(Entry& entry, const SessionSpec& spec, std::string& err) {
    Entry* e = &entry;
    // INTENT: recap is fixed at creation; SET_FILTER keeps it whatever the
    // request says (its recap byte is defined for CREATE_SESSION only).
    SessionSpec want = spec;
    want.recap = e->spec.recap;
    FilterSpec fs;
    uint64_t mask = 0;
    Pipeline* group = nullptr;
    std::vector<UnresolvedName> unresolved;
    const uint64_t version = names_version();
    if (!resolve(want, fs, mask, group, err, &unresolved)) return false;
    if (mask != e->session->partition_mask) {
        err = "session " + spec.name + ": topics cannot change after creation; destroy and recreate";
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
    e->unresolved = std::move(unresolved);
    e->names_version = version;
    report_unresolved_locked(*e);
    return true;
}

// Writes SYMBOL_UNRESOLVED for each unresolved name not reported with the same
// reason before. A name that resolved, or whose topic has no list yet, is
// forgotten, so it is reported again if it goes missing later.
void SessionManager::report_unresolved_locked(Entry& e) {
    std::vector<UnresolvedName> now;
    for (const UnresolvedName& u : e.unresolved) {
        if (!u.listed) continue;
        now.push_back(u);
        if (std::find(e.reported.begin(), e.reported.end(), u) != e.reported.end()) continue;
        SymbolUnresolvedEvent ev{};
        ev.topic_id = u.topic_id;
        ev.reason = static_cast<uint8_t>(u.reason);
        ev.name_len = static_cast<uint8_t>(u.name.size() < sizeof(ev.name) ? u.name.size() : sizeof(ev.name));
        std::memcpy(ev.name, u.name.data(), ev.name_len);
        PipelineCommand c;
        c.type = PipelineCommand::Type::EMIT;
        c.session = e.session.get();
        c.code = ControlCode::SYMBOL_UNRESOLVED;
        c.topic_id = u.topic_id;
        c.body_len = sizeof(ev);
        std::memcpy(c.body, &ev, sizeof(ev));
        std::string err;
        if (!post_and_wait(*e.group, c, err)) {
            LogRegistry::instance().warn("SESSION", "UNRESOLVED", e.spec.name + ": " + err);
            continue;
        }
        LogRegistry::instance().warn("SESSION", "UNRESOLVED", e.spec.name + ": " + u.name +
                                     (u.reason == UnresolvedReason::AMBIGUOUS ? " is listed in more than one of its topics; qualify it as <topic>:<name>"
                                                                              : " is not listed by its topics' resolvers"));
    }
    e.reported = std::move(now);
}

void SessionManager::service_names() {
    if (directories_.empty()) return;
    std::lock_guard<std::mutex> lock(mutex_);
    const uint64_t version = names_version();
    for (Entry& e : entries_) {
        if (!e.session || e.unresolved.empty() || e.names_version == version) continue;
        // INTENT: a refresh (every refresh_ms) changes the version whether or
        // not the list did; swap the filter only when a name now resolves
        // differently, so apps do not see FILTER_RELOADED for nothing.
        FilterSpec fs;
        uint64_t mask = 0;
        Pipeline* group = nullptr;
        std::vector<UnresolvedName> unresolved;
        std::string err;
        if (!resolve(e.spec, fs, mask, group, err, &unresolved)) {
            LogRegistry::instance().warn("SESSION", "NAMES", e.spec.name + ": " + err);
            continue;
        }
        if (unresolved == e.unresolved) {
            e.names_version = version;
            continue;
        }
        if (!set_filter_locked(e, e.spec, err)) LogRegistry::instance().warn("SESSION", "NAMES", e.spec.name + ": " + err);
    }
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
    return a.topics != b.topics || a.symbols != b.symbols || a.all_symbols != b.all_symbols ||
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
        s.topics = e.spec.topics;
        s.reader_flags = e.session->ring.reader_flags();
        s.reader_sample_every = e.session->ring.reader_sample_every();
        if (const ReaderSlot* slots = e.session->ring.reader_slots()) {
            for (uint32_t i = 0; i < READER_SLOTS; ++i) {
                const ReaderSlot& rs = slots[i];
                const uint32_t pid = rs.owner_pid.load(std::memory_order_acquire);
                if (pid == READER_SLOT_FREE || pid == READER_SLOT_RECLAIMING || pid > 0x7FFFFFFFu) continue;
                ReaderReport r;
                r.slot = i;
                r.pid = static_cast<int32_t>(pid);
                // INTENT: acquire pairs with the reader's release store of 1
                // after it wrote its identity; before that the fields may be
                // half-written, so only the pid is reported.
                r.identity_ready = rs.identity_ready.load(std::memory_order_acquire) == 1;
                if (r.identity_ready) {
                    r.start_time = rs.start_time;
                    r.uid = rs.uid;
                    r.sdk_version = rs.sdk_version;
                    r.capabilities = rs.capabilities;
                    // Untrusted bytes: stop at NUL or the field's end, keep printable ASCII only.
                    for (size_t k = 0; k < sizeof rs.name && rs.name[k] != '\0'; ++k) {
                        const char ch = rs.name[k];
                        r.name += (ch >= 0x20 && ch < 0x7F) ? ch : '?';
                    }
                }
                r.flags_requested = s.reader_flags | rs.flags.load(std::memory_order_relaxed);
                r.flags_active = rs.active_flags.load(std::memory_order_relaxed);
                r.read_pos = rs.read_pos.load(std::memory_order_relaxed);
                r.heartbeat_ns = rs.heartbeat_ns.load(std::memory_order_relaxed);
                r.overruns = rs.overruns.load(std::memory_order_relaxed);
                r.records = rs.records.load(std::memory_order_relaxed);
                for (uint32_t b = 0; b < READER_LAT_BUCKETS; ++b) r.lat_buckets[b] = rs.lat_buckets[b].load(std::memory_order_relaxed);
                r.lat_sum_ns = rs.lat_sum_ns.load(std::memory_order_relaxed);
                r.lat_count = rs.lat_count.load(std::memory_order_relaxed);
                s.readers.push_back(std::move(r));
            }
        }
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

uint32_t SessionManager::service_readers(uint64_t now_ns_) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (last_reader_service_ns_ != 0 && now_ns_ - last_reader_service_ns_ < 1'000'000'000ull) return 0;
    last_reader_service_ns_ = now_ns_;
    uint32_t freed = 0;
    for (auto& e : entries_) {
        ReaderSlot* slots = e.session->ring.reader_slots();
        if (slots == nullptr) continue;
        for (uint32_t i = 0; i < READER_SLOTS; ++i) {
            ReaderSlot& rs = slots[i];
            uint32_t pid = rs.owner_pid.load(std::memory_order_acquire);
            if (pid == READER_SLOT_FREE || pid == READER_SLOT_RECLAIMING) continue;
            // INTENT: both conditions. Silence alone is a reader that stopped
            // polling; a missing pid alone may be a reader in another pid
            // namespace (a container), whose pid means nothing here. Freeing
            // a live reader's slot would let a second reader write into it.
            const uint64_t hb = rs.heartbeat_ns.load(std::memory_order_relaxed);
            if (hb != 0 && (now_ns_ < hb || now_ns_ - hb < cfg_.reader_stale_ns)) continue;
            bool dead = pid > 0x7FFFFFFFu;
            if (!dead) {
                dead = ::kill(static_cast<pid_t>(pid), 0) != 0 && errno == ESRCH;
                if (!dead && rs.identity_ready.load(std::memory_order_acquire) == 1 && rs.start_time != 0) {
                    observability::Mapper id;
                    if (observability::read_identity("/proc", static_cast<int32_t>(pid), id) && id.start_time != rs.start_time) dead = true;
                }
            }
            if (!dead) continue;
            if (!rs.owner_pid.compare_exchange_strong(pid, READER_SLOT_RECLAIMING, std::memory_order_acq_rel)) continue;
            rs.flags.store(0, std::memory_order_relaxed);
            rs.sample_every.store(0, std::memory_order_relaxed);
            rs.identity_ready.store(0, std::memory_order_relaxed);
            rs.read_pos.store(0, std::memory_order_relaxed);
            rs.heartbeat_ns.store(0, std::memory_order_relaxed);
            rs.active_flags.store(0, std::memory_order_relaxed);
            // INTENT: release: the next claimer's CAS (acq_rel) sees the slot reset.
            rs.owner_pid.store(READER_SLOT_FREE, std::memory_order_release);
            ++freed;
            LogRegistry::instance().info("SESSION", "READER_SLOT_RECLAIMED",
                                         e.session->name + " slot=" + std::to_string(i) + " pid=" + std::to_string(pid));
        }
    }
    return freed;
}

bool SessionManager::set_reader_trace(const std::string& session, bool on, int32_t pid, uint32_t sample_every,
                                      uint32_t& matched, std::string& err) {
    std::lock_guard<std::mutex> lock(mutex_);
    matched = 0;
    Entry* e = find_locked(session);
    if (e == nullptr) { err = "no session " + session; return false; }
    RingWriter& ring = e->session->ring;
    if (pid == 0) {
        ring.set_reader_flags(on ? (ring.reader_flags() | READER_FLAG_TRACE_LATENCY) : (ring.reader_flags() & ~READER_FLAG_TRACE_LATENCY),
                              sample_every);
        if (ReaderSlot* slots = ring.reader_slots()) {
            for (uint32_t i = 0; i < READER_SLOTS; ++i) {
                const uint32_t o = slots[i].owner_pid.load(std::memory_order_acquire);
                if (o != READER_SLOT_FREE && o != READER_SLOT_RECLAIMING) ++matched;
            }
        }
        return true;
    }
    ReaderSlot* slots = ring.reader_slots();
    for (uint32_t i = 0; slots != nullptr && i < READER_SLOTS; ++i) {
        if (slots[i].owner_pid.load(std::memory_order_acquire) != static_cast<uint32_t>(pid)) continue;
        slots[i].sample_every.store(sample_every, std::memory_order_relaxed);
        const uint32_t f = slots[i].flags.load(std::memory_order_relaxed);
        slots[i].flags.store(on ? (f | READER_FLAG_TRACE_LATENCY) : (f & ~READER_FLAG_TRACE_LATENCY), std::memory_order_relaxed);
        ++matched;
    }
    if (matched == 0) { err = "pid " + std::to_string(pid) + " holds no reader slot on session " + session; return false; }
    return true;
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
