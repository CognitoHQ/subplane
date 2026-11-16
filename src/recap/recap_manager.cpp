#include "subplane/recap/recap_manager.h"

#include "subplane/core/clock.h"
#include "subplane/logging/log_registry.h"
#include "subplane/pipeline/partition.h"
#include "subplane/proto/subplane_proto_codec.h"
#include "subplane/symbol_table.h"

#include <chrono>
#include <cstring>
#include <algorithm>
#include <inplace_vector>
#include <sched.h>

namespace subplane {

namespace {
constexpr int kFragmentsPerPoll = 64;
constexpr int kCommandsPerLoop = 4096;
constexpr uint64_t kOverflowLogIntervalNs = 10'000'000'000ull;
constexpr uint64_t kStatusReasonBase = 0x100; // RecapReply.reason for a Status: base + code
constexpr std::string_view kServiceName = "subplaned";
constexpr uint16_t kRecapRequestBulk = 1u << 0; // RecapRequest.Flags bit 0: every instrument of the partition
constexpr uint16_t kResolveReplyLast = 1u << 0; // ResolveReply.Flags bit 0: the final reply to a Resolve
constexpr uint64_t kNsPerMs = 1'000'000ull;

} // namespace

RecapManager::RecapManager(uint64_t epoch, SymbolTable& symbols)
    : epoch_(epoch), symbols_(symbols), rng_(epoch ? epoch : 0x9E3779B97F4A7C15ull) {}

RecapManager::~RecapManager() { stop(); }

bool RecapManager::add_partition(Partition& p, RecapChannel& ch, std::string& err) {
    if (!p.has_recap()) { err = "partition " + p.name() + " has no recap block"; return false; }
    if (find(p.id())) { err = "partition " + p.name() + " already added to the recap manager"; return false; }
    auto ps = std::make_unique<PState>();
    ps->mgr = this;
    ps->part = &p;
    ps->ch = &ch;
    ps->cfg = p.recap_config();
    ps->transport = p.take_recap_transport();
    if (!ps->transport) { err = "partition " + p.name() + ": recap component already taken"; return false; }
    // INTENT: every reply buffer exists before the first request (INV-002 on
    // the pipeline side relies on slabs never moving while it merges).
    ps->slabs.resize(ps->cfg.reply_slabs);
    for (auto& s : ps->slabs) s.assign(ps->cfg.reply_slab_bytes, 0);
    for (uint32_t i = ps->cfg.reply_slabs; i > 0; --i) ps->free_slabs.push_back(static_cast<uint16_t>(i - 1));
    ps->entries.reserve(ps->cfg.max_inflight * 2u);
    parts_.push_back(std::move(ps));
    bool known = false;
    for (RecapChannel* c : channels_) known = known || c == &ch;
    if (!known) channels_.push_back(&ch);
    return true;
}

bool RecapManager::add_resolver(Partition& p, RecapChannel& ch, TopicDirectory& dir, uint16_t partitions, std::string& err) {
    if (!p.has_resolver()) { err = "topic " + p.topic_name() + " has no resolver block"; return false; }
    auto rs = std::make_unique<RState>();
    rs->mgr = this;
    rs->part = &p;
    rs->ch = &ch;
    rs->dir = &dir;
    rs->configured = partitions;
    rs->cfg = p.resolver_config();
    rs->transport = p.take_resolver_transport();
    if (!rs->transport) { err = "topic " + p.topic_name() + ": resolver component already taken"; return false; }
    resolvers_.push_back(std::move(rs));
    return true;
}

bool RecapManager::open(std::string& err) {
    for (auto& rs : resolvers_) {
        if (!rs->transport->open(err)) {
            err = "topic " + rs->part->topic_name() + ": resolver: " + err;
            return false;
        }
        LogRegistry::instance().info("RESOLVER", "OPEN", rs->part->topic_name() + " " + rs->transport->describe());
    }
    for (auto& ps : parts_) {
        if (!ps->transport->open(err)) {
            err = "partition " + ps->part->name() + ": recap: " + err;
            return false;
        }
        LogRegistry::instance().info("RECAP", "OPEN", ps->part->name() + " " + ps->transport->describe());
    }
    return true;
}

void RecapManager::start() {
    if (running_ || (parts_.empty() && resolvers_.empty())) return;
    stop_.store(false, std::memory_order_release);
    thread_ = std::thread([this] { loop(); });
    running_ = true;
}

void RecapManager::stop() {
    if (running_) {
        stop_.store(true, std::memory_order_release);
        if (thread_.joinable()) thread_.join();
        running_ = false;
    }
    for (auto& ps : parts_) if (ps->transport) ps->transport->close();
    for (auto& rs : resolvers_) if (rs->transport) rs->transport->close();
}

void RecapManager::loop() noexcept {
    // INTENT: recap is cold. Spin briefly after work (a reply arrives in many
    // fragments), then yield, then sleep 1 ms so an idle manager costs nothing.
    int idle = 0;
    while (!stop_.load(std::memory_order_acquire)) {
        if (run_once(now_mono_ns()) > 0) { idle = 0; continue; }
        ++idle;
        if (idle < 100) continue;
        if (idle < 200) { sched_yield(); continue; }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

RecapManager::PState* RecapManager::find(uint16_t partition) noexcept {
    for (auto& ps : parts_) if (ps->part->id() == partition) return ps.get();
    return nullptr;
}

int RecapManager::run_once(uint64_t now) noexcept {
    int work = 0;
    for (RecapChannel* ch : channels_) {
        RecapRelease rel;
        while (ch->releases.pop(rel)) {
            if (PState* ps = find(rel.partition); ps && rel.slab < ps->slabs.size()) ps->free_slabs.push_back(rel.slab);
            ++work;
        }
        RecapCommand c;
        for (int i = 0; i < kCommandsPerLoop && ch->requests.pop(c); ++i) {
            if (PState* ps = find(c.partition)) { ps->now = now; handle_command(*ps, c); }
            ++work;
        }
    }
    for (auto& ps : parts_) {
        ps->now = now;
        // INTENT: SPP section 5: Hello is the first message on every
        // connection, and the peer's state is per connection. A reconnect may
        // be a different (older or newer) service, so its Hello is forgotten.
        const bool conn = ps->transport->connected();
        if (conn != ps->was_connected) {
            ps->was_connected = conn;
            ps->hello_sent = false;
            ps->peer = Peer{};
            for (size_t src = 0; src < kRecapSources; ++src)   // in-flight requests time out and retry
                if (ps->asm_[src].active || ps->asm_[src].skipping) abort_assembly(*ps, static_cast<RecapSource>(src));
        }
        bool hold_requests = false;
        if (conn && !ps->hello_sent) {
            ps->hello_sent = send_hello(*ps);
            hold_requests = !ps->hello_sent; // back-pressured: the Hello goes first next loop
        }
        work += ps->transport->poll(*ps, kFragmentsPerPoll);
        service_timeouts(*ps);
        if (!hold_requests) { send_ready(*ps); send_bulk(*ps); }
        publish_gauges(*ps);
    }
    for (auto& rs : resolvers_) {
        rs->now = now;
        work += rs->transport->poll(*rs, kFragmentsPerPoll);
        run_resolver(*rs);
    }
    flush_outbox();
    return work;
}

// ---- resolver client ----

void RecapManager::run_resolver(RState& rs) noexcept {
    ResolverCounters& rc = rs.part->resolver_counters;
    const bool conn = rs.transport->connected();
    if (conn != rs.was_connected) {
        // As for recap (SPP section 5): the peer is per connection, and a
        // reconnect may be a restarted service with a new list: ask at once.
        rs.was_connected = conn;
        rs.hello_sent = false;
        rs.peer = Peer{};
        rs.rid = 0;
        rs.due_ns = 0;
        rc.connected.store(conn ? 1 : 0, std::memory_order_relaxed);
    }
    if (!conn) return;
    if (!rs.hello_sent) {
        sp_proto::HelloEncoder e(rs.tx + 4, sizeof(rs.tx) - 4);
        e.CorrelationId(next_request_id_++).PeerEpoch(epoch_).ProtoVersion(sp_proto::kSchemaVersion).MinProtoVersion(1)
         .Role(sp_proto::Role::Agent).ServiceName(kServiceName);
        e.begin_Extensions(0);
        const auto n = static_cast<uint32_t>(e.finish());
        std::memcpy(rs.tx, &n, 4);
        if (rs.transport->send(rs.tx, 4 + n) == RecapSendStatus::BACK_PRESSURED) return;
        rs.hello_sent = true;
    }
    if (rs.rid != 0) {
        if (rs.now < rs.due_ns) return;
        // INTENT: a whole topic is several replies; one lost page leaves the
        // list incomplete, so the whole answer is asked for again.
        inc(rc.resolver_timeouts);
        rs.rid = 0;
        rs.due_ns = rs.now + static_cast<uint64_t>(rs.cfg.timeout_ms) * kNsPerMs;
        return;
    }
    if (rs.now < rs.due_ns || rs.peer.incompatible) return;
    const uint64_t rid = next_request_id_;
    sp_proto::ResolveEncoder e(rs.tx + 4, sizeof(rs.tx) - 4);
    e.CorrelationId(rid).Instrument(0).Venue(0).Flags(0).Name(std::string_view{});
    const auto n = static_cast<uint32_t>(e.finish());
    std::memcpy(rs.tx, &n, 4);
    const RecapSendStatus st = rs.transport->send(rs.tx, 4 + n);
    if (st == RecapSendStatus::BACK_PRESSURED) return;
    if (st != RecapSendStatus::OK) {
        rs.due_ns = rs.now + static_cast<uint64_t>(rs.cfg.timeout_ms) * kNsPerMs;
        return;
    }
    ++next_request_id_;
    inc(rc.resolver_requests);
    rs.rid = rid;
    rs.due_ns = rs.now + static_cast<uint64_t>(rs.cfg.timeout_ms) * kNsPerMs;
    rs.building.clear();
    rs.got_topic = rs.got_parts = 0;
}

void RecapManager::RState::on_fragment(const uint8_t* data, uint32_t len, RecapSource) noexcept {
    uint32_t at = 0;
    while (at < len) {
        uint32_t mlen = 0;
        if (len - at < 4) { inc(part->resolver_counters.resolver_stray); return; }
        std::memcpy(&mlen, data + at, 4);
        at += 4;
        if (mlen > len - at) { inc(part->resolver_counters.resolver_stray); return; }
        mgr->on_resolver_message(*this, data + at, mlen);
        at += mlen;
    }
}

void RecapManager::on_resolver_message(RState& rs, const uint8_t* msg, uint32_t len) noexcept {
    ResolverCounters& rc = rs.part->resolver_counters;
    sp_proto::Header h;
    if (!sp_proto::read_header(msg, len, h) || h.schema_id != sp_proto::kSchemaId) { inc(rc.resolver_stray); return; }
    switch (h.template_id) {
        case sp_proto::HelloDecoder::kTemplateId: {
            sp_proto::HelloDecoder d;
            if (!d.wrap(msg, len)) break;
            rs.peer.hello = true;
            rs.peer.epoch = d.PeerEpoch();
            rs.peer.service.assign(d.ServiceName());
            rs.peer.incompatible = d.MinProtoVersion() > sp_proto::kSchemaVersion;
            LogRegistry::instance().info("RESOLVER", "HELLO", rs.part->topic_name() + ": " +
                                         (rs.peer.service.empty() ? std::string("resolver") : rs.peer.service) +
                                         (rs.peer.incompatible ? " speaks a newer SPP only; resolving is off until it reconnects" : ""));
            return;
        }
        case sp_proto::ResolveReplyDecoder::kTemplateId: {
            sp_proto::ResolveReplyDecoder d;
            if (!d.wrap(msg, len)) break;
            if (rs.rid == 0 || d.CorrelationId() != rs.rid) { inc(rc.resolver_stray); return; }
            inc(rc.resolver_replies);
            rs.got_topic = d.TopicId();
            rs.got_parts = d.Partitions();
            for (uint16_t i = 0; i < d.Instruments_count(); ++i) {
                const auto v = d.Instruments(i);
                rs.building.push_back(ResolvedInstrument{v.Instrument(), v.Venue(), v.Partition(), std::string(v.Symbol())});
            }
            if (d.Flags() & kResolveReplyLast) resolver_complete(rs);
            return;
        }
        case sp_proto::RecapRejectDecoder::kTemplateId:
        case sp_proto::StatusDecoder::kTemplateId: {
            uint64_t cid = 0;
            if (h.block_length < 8 || len < sp_proto::kHeaderSize + 8) break;
            std::memcpy(&cid, msg + sp_proto::kHeaderSize, 8);
            if (rs.rid == 0 || cid != rs.rid) { inc(rc.resolver_stray); return; }
            // The service does not resolve (an older snap answers Unsupported):
            // asked again at the next refresh, in case it is replaced.
            inc(rc.resolver_rejects);
            LogRegistry::instance().warn("RESOLVER", "REFUSED", rs.part->topic_name() + ": the resolver refused Resolve; asking again in " +
                                         std::to_string(rs.cfg.refresh_ms) + " ms");
            rs.rid = 0;
            rs.due_ns = rs.now + static_cast<uint64_t>(rs.cfg.refresh_ms) * kNsPerMs;
            return;
        }
        default:
            inc(rc.resolver_stray);
            return;
    }
    inc(rc.resolver_stray);
}

void RecapManager::resolver_complete(RState& rs) noexcept {
    ResolverCounters& rc = rs.part->resolver_counters;
    Partition& p = *rs.part;
    inc(rc.resolver_lists);
    rc.instruments.store(rs.building.size(), std::memory_order_relaxed);
    const bool mismatch = rs.got_topic != p.topic_id() || rs.got_parts != rs.configured;
    if (!rs.noted || rs.got_topic != rs.noted_topic || rs.got_parts != rs.noted_parts) {
        if (mismatch) {
            inc(rc.resolver_mismatch);
            LogRegistry::instance().error("RESOLVER", "MISMATCH", p.topic_name() + ": the resolver has topic " + std::to_string(rs.got_topic) +
                                          " with " + std::to_string(rs.got_parts) + " partitions; the config has topic " +
                                          std::to_string(p.topic_id()) + " with " + std::to_string(rs.configured));
        }
        ResolveNote n;
        n.kind = ResolveNote::Kind::TOPIC;
        n.mismatch = mismatch;
        n.partition = p.id();
        n.topic_id = rs.got_topic;
        n.partitions = rs.got_parts;
        push_note(*rs.ch, n);
        rs.noted = true;
        rs.noted_topic = rs.got_topic;
        rs.noted_parts = rs.got_parts;
    }
    // INTENT: intern every listed instrument now, so a session naming it and
    // the decoder seeing it later agree on one symbol id.
    for (const ResolvedInstrument& r : rs.building) {
        uint32_t sym = 0;
        if (p.symbol_passthrough()) sym = r.instrument <= 0xFFFFFFFFull ? static_cast<uint32_t>(r.instrument) : 0;
        else sym = symbols_.intern_u64(r.instrument, p.symbol_venue_scope() ? r.venue : uint16_t{0});
        if (sym == 0 || r.partition_num == 0) continue;
        auto [it, fresh] = rs.noted_home.try_emplace(sym, r.partition_num);
        if (!fresh && it->second == r.partition_num) continue;
        it->second = r.partition_num;
        ResolveNote n;
        n.kind = ResolveNote::Kind::HOME;
        n.partition = p.id();
        n.symbol_id = sym;
        n.home_num = r.partition_num;
        push_note(*rs.ch, n);
    }
    rs.dir->publish(std::move(rs.building));
    rs.building = {};
    rs.rid = 0;
    rs.due_ns = rs.now + static_cast<uint64_t>(rs.cfg.refresh_ms) * kNsPerMs;
}

void RecapManager::push_note(RecapChannel& ch, const ResolveNote& n) noexcept {
    // INTENT: a whole topic can be thousands of notes, more than the queue
    // holds; they wait here in order, counted per channel so neither this
    // nor the flush walks the backlog for every note.
    uint32_t& waiting = note_waiting_[&ch];
    if (waiting != 0 || !ch.resolved.push(n)) {
        note_outbox_.emplace_back(&ch, n);
        ++waiting;
    }
}

void RecapManager::handle_command(PState& ps, const RecapCommand& c) noexcept {
    RecapCounters& rc = ps.part->recap_counters;
    auto it = ps.entries.find(c.symbol_id);
    if (c.op == RecapCommand::Op::FLOOR) {
        // INTENT: a request posted by a restart carries the old stream's
        // floor; the pipeline rebases it at the new stream's first sequence.
        // Without this, every section of the new stream is skipped as older
        // than the floor, and recovery waits for a timeout.
        if (it != ps.entries.end()) it->second.floor = c.floor;
        return;
    }
    if (c.op == RecapCommand::Op::REQUEST) {
        if (it != ps.entries.end()) {
            it->second.cancelled = false;
            it->second.floor = c.floor;
            if (it->second.generation != c.generation) renew(ps, c.symbol_id, it->second, c.generation, c.bulk);
            else inc(rc.recap_coalesced);
            return;
        }
        Entry e;
        e.generation = c.generation;
        e.floor = c.floor;
        // A bulk already out may have passed this symbol: one ticker more goes on its own.
        e.bulk = c.bulk && ps.bulk_rid == 0;
        // INTENT: the gather window. Coalescing alone only joins requests that
        // overlap one in flight, and a recap on a fast link completes in less
        // than the few milliseconds between sessions created together (a
        // reload, apps restarting with the agent): each would send its own and
        // the snapshot service would build the same book again for each.
        e.due_ns = ps.now + static_cast<uint64_t>(ps.cfg.coalesce_ms) * 1'000'000ull;
        ps.entries.emplace(c.symbol_id, e);
        if (e.bulk) {
            if (ps.bulk_queued++ == 0) ps.bulk_due = e.due_ns;
        } else {
            ps.fifo.push_back(c.symbol_id);
        }
        return;
    }
    // CANCEL: no session waits on the symbol any more.
    if (it == ps.entries.end()) return;
    inc(rc.recap_cancelled);
    if (it->second.state == Entry::State::INFLIGHT && !it->second.bulk) it->second.cancelled = true; // ends with its reply or timeout
    else finish(ps, c.symbol_id, true);                                                                // fifo skips it lazily
}

void RecapManager::renew(PState& ps, uint32_t symbol_id, Entry& e, uint32_t generation, bool bulk) noexcept {
    inc(ps.part->recap_counters.recap_renewed);
    // INTENT: a reply (or section) being assembled for the symbol is from
    // before the change: skipped to its end.
    abort_assembly_of(ps, symbol_id);
    if (e.state == Entry::State::INFLIGHT && e.bulk) {
        // The stream changed under the bulk: every entry on it renews, so the
        // bulk is abandoned and the next one asks again.
        reset_bulk(ps);
        --ps.bulk_queued;   // counted again below
    } else if (e.state == Entry::State::INFLIGHT) {
        // INTENT: forget the request id, so its reply, now or later, is stray.
        ps.by_request.erase(e.request_id);
        --ps.inflight;
    } else if (e.state == Entry::State::QUEUED && e.bulk) {
        --ps.bulk_queued;
    }
    e.generation = generation;
    e.attempt = 0;
    e.state = Entry::State::QUEUED;
    e.due_ns = 0;
    e.request_id = 0;
    e.bulk = bulk && ps.bulk_rid == 0;
    if (e.bulk) {
        if (ps.bulk_queued++ == 0) ps.bulk_due = 0;   // a renew does not wait
    } else {
        ps.fifo.push_front(symbol_id); // ahead of first requests: a renewed symbol has waited already
    }
}

void RecapManager::reset_bulk(PState& ps) noexcept {
    for (auto& [sym, e] : ps.entries) {
        if (!e.bulk || e.state != Entry::State::INFLIGHT) continue;
        e.state = Entry::State::QUEUED;
        e.request_id = 0;
        ++ps.bulk_queued;
    }
    ps.bulk_inflight = 0;
    ps.bulk_rid = 0;
    ps.bulk_due = 0;
}

void RecapManager::send_bulk(PState& ps) noexcept {
    RecapCounters& rc = ps.part->recap_counters;
    if (ps.bulk_queued == 0 || ps.bulk_rid != 0 || ps.bulk_due > ps.now) return;
    const uint64_t rid = next_request_id_;
    RecapSendStatus st = RecapSendStatus::FAILED;
    if (!ps.peer.incompatible) {
        sp_proto::RecapRequestEncoder e(ps.tx + 4, sizeof(ps.tx) - 4);
        e.CorrelationId(rid)
         .AgentEpoch(epoch_)
         .LastSeqSeen(ps.part->last_seq_pub.load(std::memory_order_relaxed))
         .Flags(kRecapRequestBulk);
        st = send_frame(ps, static_cast<uint32_t>(e.finish()));
    }
    if (st == RecapSendStatus::BACK_PRESSURED) return; // the same bulk next loop
    if (st != RecapSendStatus::OK) {
        inc(rc.recap_send_failed);
        std::vector<uint32_t> queued;
        for (const auto& [sym, e] : ps.entries) if (e.bulk && e.state == Entry::State::QUEUED) queued.push_back(sym);
        for (const uint32_t sym : queued) fail(ps, sym, RecapStatus::SEND_FAILED, 0);
        return;
    }
    ++next_request_id_;
    for (auto& [sym, e] : ps.entries) {
        if (!e.bulk || e.state != Entry::State::QUEUED) continue;
        e.state = Entry::State::INFLIGHT;
        e.request_id = rid;
    }
    ps.bulk_inflight += ps.bulk_queued;
    ps.bulk_queued = 0;
    ps.bulk_rid = rid;
    ps.bulk_due = ps.now + static_cast<uint64_t>(ps.cfg.timeout_ms) * 1'000'000ull;
    inc(rc.recap_requests);
    inc(rc.recap_bulk_requests);
}

uint64_t RecapManager::backoff_ns(const PState& ps, uint32_t attempt) noexcept {
    // INTENT: exponential with jitter in [cap/2, cap]: a box's retries spread
    // out so many agents do not hit a recovering service in lockstep, and a
    // retry never comes back immediately.
    const uint32_t shift = attempt > 0 ? (attempt - 1 < 20 ? attempt - 1 : 20) : 0;
    uint64_t cap_ms = static_cast<uint64_t>(ps.cfg.retry_base_ms) << shift;
    if (cap_ms > ps.cfg.retry_max_ms) cap_ms = ps.cfg.retry_max_ms;
    rng_ ^= rng_ << 13; rng_ ^= rng_ >> 7; rng_ ^= rng_ << 17; // xorshift64
    const uint64_t cap_ns = cap_ms * 1'000'000ull;
    return cap_ns / 2 + rng_ % (cap_ns / 2 + 1);
}

void RecapManager::send_ready(PState& ps) noexcept {
    RecapCounters& rc = ps.part->recap_counters;
    while (ps.inflight < ps.cfg.max_inflight && !ps.fifo.empty()) {
        const uint32_t sym = ps.fifo.front();
        auto it = ps.entries.find(sym);
        if (it == ps.entries.end() || it->second.state != Entry::State::QUEUED || it->second.bulk) { ps.fifo.pop_front(); continue; }
        // First requests join the back in arrival order, so when the front is
        // still gathering so is everything behind it. A retry queued behind it
        // waits at most coalesce_ms longer; a renew goes to the front, due now.
        if (it->second.due_ns > ps.now) break;

        const uint64_t rid = next_request_id_;
        uint64_t instrument = 0;
        uint16_t venue = 0;
        if (ps.part->symbol_passthrough()) {
            instrument = sym;
        } else {
            instrument = symbols_.key_u64(sym);
            if (ps.part->symbol_venue_scope()) venue = static_cast<uint16_t>(symbols_.venue_u64(sym));
        }
        RecapSendStatus st = RecapSendStatus::FAILED;
        if (!ps.peer.incompatible) {
            sp_proto::RecapRequestEncoder e(ps.tx + 4, sizeof(ps.tx) - 4);
            e.CorrelationId(rid)
             .AgentEpoch(epoch_)
             .Instrument(instrument)
             .Venue(venue)
             .LastSeqSeen(ps.part->last_seq_pub.load(std::memory_order_relaxed));
            st = send_frame(ps, static_cast<uint32_t>(e.finish()));
        }
        if (st == RecapSendStatus::BACK_PRESSURED) break; // same request next loop
        ps.fifo.pop_front();
        if (st != RecapSendStatus::OK) {
            inc(rc.recap_send_failed);
            fail(ps, sym, RecapStatus::SEND_FAILED, 0);
            continue;
        }
        ++next_request_id_;
        Entry& e = it->second;
        e.state = Entry::State::INFLIGHT;
        e.request_id = rid;
        e.due_ns = ps.now + static_cast<uint64_t>(ps.cfg.timeout_ms) * 1'000'000ull;
        ps.by_request[rid] = sym;
        ++ps.inflight;
        inc(rc.recap_requests);
    }
}

void RecapManager::service_timeouts(PState& ps) noexcept {
    RecapCounters& rc = ps.part->recap_counters;
    // Collected first: fail() and finish() change the map.
    // A full batch goes round again for the rest.
    std::inplace_vector<uint32_t, 64> expired;
    for (;;) {
        expired.clear();
        // INTENT: the bulk's deadline is silence on the section stream, not
        // its total time (reply_begin pushes it back): a paced burst of a
        // whole partition takes as long as it takes, and the entries still
        // waiting when it goes quiet were not in it.
        const bool bulk_expired = ps.bulk_rid != 0 && ps.now >= ps.bulk_due;
        for (auto& [sym, e] : ps.entries) {
            const bool expired_now = e.state == Entry::State::INFLIGHT && (e.bulk ? bulk_expired : ps.now >= e.due_ns);
            if (expired_now) { if (!expired.try_push_back(sym)) break; }
            else if (e.state == Entry::State::BACKOFF && ps.now >= e.due_ns) { e.state = Entry::State::QUEUED; ps.fifo.push_back(sym); }
        }
        for (const uint32_t sym : expired) {
            abort_assembly_of(ps, sym);
            inc(rc.recap_timeouts);
            fail(ps, sym, RecapStatus::TIMEOUT, 0);
        }
        if (expired.size() < expired.capacity()) break;
    }
}

void RecapManager::fail(PState& ps, uint32_t symbol_id, RecapStatus st, uint64_t reason) noexcept {
    auto it = ps.entries.find(symbol_id);
    if (it == ps.entries.end()) return;
    if (!it->second.cancelled) {
        RecapReply r;
        r.partition = ps.part->id();
        r.symbol_id = symbol_id;
        r.request_id = it->second.request_id;
        r.attempt = it->second.attempt + 1;
        r.generation = it->second.generation;
        r.status = st;
        r.reason = reason;
        push_reply(ps, r);
    }
    finish(ps, symbol_id, false);
}

void RecapManager::finish(PState& ps, uint32_t symbol_id, bool success) noexcept {
    auto it = ps.entries.find(symbol_id);
    if (it == ps.entries.end()) return;
    Entry& e = it->second;
    if (e.state == Entry::State::INFLIGHT && e.bulk) {
        if (--ps.bulk_inflight == 0) ps.bulk_rid = 0;
    } else if (e.state == Entry::State::INFLIGHT) {
        --ps.inflight;
        ps.by_request.erase(e.request_id);
    } else if (e.state == Entry::State::QUEUED && e.bulk) {
        --ps.bulk_queued;
    }
    // INTENT: retry for as long as a session waits (no attempt limit); the
    // pipeline cancels when the last waiter leaves, and every failure was
    // already reported so apps see RECAP_FAILED each time.
    if (success || e.cancelled) {
        ps.entries.erase(it);
        return;
    }
    ++e.attempt;
    e.bulk = false;   // a retry is one ticker: the bulk it missed is over
    e.request_id = 0;
    e.state = Entry::State::BACKOFF;
    e.due_ns = ps.now + backoff_ns(ps, e.attempt);
}

void RecapManager::abort_assembly(PState& ps, RecapSource src) noexcept {
    Assembly& a = ps.asm_[static_cast<size_t>(src)];
    if (a.slab >= 0) ps.free_slabs.push_back(static_cast<uint16_t>(a.slab));
    a = Assembly{};
}

void RecapManager::abort_assembly_of(PState& ps, uint32_t symbol_id) noexcept {
    for (size_t src = 0; src < kRecapSources; ++src) {
        if (!ps.asm_[src].active || ps.asm_[src].symbol_id != symbol_id) continue;
        abort_assembly(ps, static_cast<RecapSource>(src));
        ps.asm_[src].skipping = true;
    }
}

RecapSendStatus RecapManager::send_frame(PState& ps, uint32_t msg_len) noexcept {
    if (msg_len == 0) return RecapSendStatus::FAILED; // the encoder ran out of room: a bug, never sent half
    std::memcpy(ps.tx, &msg_len, 4); // INTENT: SPP section 4: every message carries a u32 LE length, so a reader can skip it whole
    return ps.transport->send(ps.tx, 4 + msg_len);
}

bool RecapManager::send_hello(PState& ps) noexcept {
    sp_proto::HelloEncoder e(ps.tx + 4, sizeof(ps.tx) - 4);
    e.CorrelationId(next_request_id_++)
     .PeerEpoch(epoch_)
     .ProtoVersion(sp_proto::kSchemaVersion)
     .MinProtoVersion(1)
     .Role(sp_proto::Role::Agent)
     .ServiceName(kServiceName);
    e.begin_Extensions(0);
    const RecapSendStatus st = send_frame(ps, static_cast<uint32_t>(e.finish()));
    // Not connected or failed: the requests fail on their own send and retry;
    // the next connection sends the Hello again.
    return st != RecapSendStatus::BACK_PRESSURED;
}

void RecapManager::send_status(PState& ps, uint64_t correlation_id, uint16_t code) noexcept {
    if (correlation_id == 0) return; // SPP section 3: answered only when the sender can match it
    sp_proto::StatusEncoder e(ps.tx + 4, sizeof(ps.tx) - 4);
    e.CorrelationId(correlation_id).Code(static_cast<sp_proto::StatusCode>(code));
    (void)send_frame(ps, static_cast<uint32_t>(e.finish())); // best effort: the sender times out otherwise
}

void RecapManager::PState::on_fragment(const uint8_t* data, uint32_t len, RecapSource src) noexcept {
    if (cfg.reply == RecapReplyMode::SPP || src == RecapSource::SECTIONS) {
        // INTENT: a frame is u32-length-prefixed SPP messages back to back. A
        // bad length ends the frame: nothing after it can be trusted.
        uint32_t at = 0;
        while (at < len) {
            uint32_t mlen = 0;
            if (len - at < 4) { inc(part->recap_counters.recap_stray); return; }
            std::memcpy(&mlen, data + at, 4);
            at += 4;
            if (mlen > len - at) { inc(part->recap_counters.recap_stray); return; }
            mgr->on_spp_message(*this, src, data + at, mlen);
            at += mlen;
        }
        return;
    }
    const PacketView pkt{data, len, 0};
    FrameInfo info;
    MessageIter it;
    const Framer& fr = part->framer();
    if (!fr.parse(pkt, info, it)) { inc(part->recap_counters.recap_stray); return; }
    const uint8_t* msg = nullptr;
    uint32_t mlen = 0;
    for (;;) {
        const NextStatus st = fr.next(it, msg, mlen);
        if (st == NextStatus::OK) { mgr->on_marker_message(*this, msg, mlen); continue; }
        if (st != NextStatus::END) inc(part->recap_counters.recap_stray);
        break;
    }
}

void RecapManager::on_spp_message(PState& ps, RecapSource src, const uint8_t* msg, uint32_t len) noexcept {
    RecapCounters& rc = ps.part->recap_counters;
    sp_proto::Header h;
    if (!sp_proto::read_header(msg, len, h) || h.schema_id != sp_proto::kSchemaId) { inc(rc.recap_stray); return; }
    switch (h.template_id) {
        case sp_proto::HelloDecoder::kTemplateId:
            on_peer_hello(ps, msg, len);
            return;
        case sp_proto::RecapBeginDecoder::kTemplateId: {
            sp_proto::RecapBeginDecoder d;
            if (!d.wrap(msg, len)) break;
            BeginInfo b;
            b.has_id = b.has_as_of = true;
            b.rid = d.CorrelationId();
            b.as_of = d.AsOf();
            b.has_epoch = d.SourceEpoch() != 0;
            b.epoch = d.SourceEpoch();
            b.instrument = d.Instrument();
            b.venue = d.Venue();
            reply_begin(ps, src, b);
            return;
        }
        case sp_proto::RecapRecordDecoder::kTemplateId: {
            sp_proto::RecapRecordDecoder d;
            if (!d.wrap(msg, len)) break;
            const Assembly& a = ps.asm_[static_cast<size_t>(src)];
            if (a.active && d.CorrelationId() != a.request_id) { inc(rc.recap_stray); return; } // replies are never interleaved
            reply_record(ps, src, d.Payload_data(), static_cast<uint32_t>(d.Payload_size()));
            return;
        }
        case sp_proto::RecapEndDecoder::kTemplateId: {
            sp_proto::RecapEndDecoder d;
            if (!d.wrap(msg, len)) break;
            // INTENT: RecordCount guards against a reply that lost records on
            // the way; a short snapshot would be merged as if complete. The
            // request then times out and is retried.
            const Assembly& a = ps.asm_[static_cast<size_t>(src)];
            if (a.active && d.CorrelationId() == a.request_id && !a.overflow && d.RecordCount() != a.messages) {
                inc(rc.recap_stray);
                abort_assembly(ps, src);
                return;
            }
            reply_end(ps, src, true, d.CorrelationId());
            return;
        }
        case sp_proto::RecapRejectDecoder::kTemplateId: {
            sp_proto::RecapRejectDecoder d;
            if (!d.wrap(msg, len)) break;
            reply_reject(ps, true, d.CorrelationId(), static_cast<uint64_t>(d.Reason()));
            return;
        }
        case sp_proto::StatusDecoder::kTemplateId: {
            sp_proto::StatusDecoder d;
            if (!d.wrap(msg, len)) break;
            const uint16_t code = static_cast<uint16_t>(d.Code());
            const bool ours = ps.by_request.count(d.CorrelationId()) != 0 || (ps.bulk_rid != 0 && d.CorrelationId() == ps.bulk_rid);
            if (ours && code == static_cast<uint16_t>(sp_proto::StatusCode::Ok)) return; // a service's acknowledgement: the sections follow
            if (ours) {
                // The service could not even start a reply (Unsupported, Busy, ...): as a reject.
                reply_reject(ps, true, d.CorrelationId(), kStatusReasonBase + code);
                return;
            }
            if (code == static_cast<uint16_t>(sp_proto::StatusCode::Unsupported)) {
                LogRegistry::instance().warn("RECAP", "UNSUPPORTED", ps.part->name() + ": the snapshot service answered Unsupported to message " +
                                             std::to_string(d.CorrelationId()));
            }
            inc(rc.recap_stray);
            return;
        }
        case sp_proto::ExtensionDecoder::kTemplateId: {
            sp_proto::ExtensionDecoder d;
            if (!d.wrap(msg, len)) break;
            // SPP section 7: the agent implements no extension on this link.
            if (d.Flags() & 1u) send_status(ps, d.CorrelationId(), static_cast<uint16_t>(sp_proto::StatusCode::Unsupported));
            return;
        }
        default: {
            // SPP section 3: an unknown (or, here, unexpected) core message is
            // answered Unsupported and counted; it never ends the link.
            uint64_t cid = 0;
            if (h.block_length >= 8 && len >= sp_proto::kHeaderSize + 8) std::memcpy(&cid, msg + sp_proto::kHeaderSize, 8);
            send_status(ps, cid, static_cast<uint16_t>(sp_proto::StatusCode::Unsupported));
            inc(rc.recap_stray);
            return;
        }
    }
    inc(rc.recap_stray); // a known template whose lengths do not fit the frame
}

void RecapManager::on_peer_hello(PState& ps, const uint8_t* msg, uint32_t len) noexcept {
    sp_proto::HelloDecoder d;
    if (!d.wrap(msg, len)) { inc(ps.part->recap_counters.recap_stray); return; }
    Peer& p = ps.peer;
    p.hello = true;
    p.epoch = d.PeerEpoch();
    p.service.assign(d.ServiceName());
    const uint16_t theirs = d.ProtoVersion() == 0 ? 1 : d.ProtoVersion();
    p.version = theirs < sp_proto::kSchemaVersion ? theirs : sp_proto::kSchemaVersion;
    const std::string range = std::to_string(d.MinProtoVersion()) + ".." + std::to_string(theirs);
    if (d.MinProtoVersion() > sp_proto::kSchemaVersion) {
        p.incompatible = true;
        send_status(ps, d.CorrelationId(), static_cast<uint16_t>(sp_proto::StatusCode::Unsupported));
        LogRegistry::instance().error("RECAP", "VERSION", ps.part->name() + ": snapshot service " + p.service + " speaks SPP " + range +
                                      ", the agent 1.." + std::to_string(sp_proto::kSchemaVersion) + "; recap is off until it reconnects");
        return;
    }
    p.incompatible = false;
    LogRegistry::instance().info("RECAP", "HELLO", ps.part->name() + ": " + (p.service.empty() ? std::string("snapshot service") : p.service) +
                                 " SPP " + range + ", using " + std::to_string(p.version));
}

void RecapManager::on_marker_message(PState& ps, const uint8_t* msg, uint32_t len) noexcept {
    RecapMarkerFields f;
    if (!ps.part->read_recap_fields(msg, len, f)) { reply_record(ps, RecapSource::RESPONSE, msg, len); return; }
    const bool has_id = f.has[RECAP_REQUEST_ID];
    const uint64_t rid = f.value[RECAP_REQUEST_ID];
    switch (f.marker) {
        case RecapMarker::BEGIN: {
            BeginInfo b;
            b.has_id = has_id;
            b.rid = rid;
            b.has_as_of = f.has[RECAP_AS_OF];
            b.as_of = f.value[RECAP_AS_OF];
            b.has_epoch = f.has[RECAP_SOURCE_EPOCH];
            b.epoch = f.value[RECAP_SOURCE_EPOCH];
            reply_begin(ps, RecapSource::RESPONSE, b);
            return;
        }
        case RecapMarker::END:
            reply_end(ps, RecapSource::RESPONSE, has_id, rid);
            return;
        case RecapMarker::REJECT:
            reply_reject(ps, has_id, rid, f.has[RECAP_REASON] ? f.value[RECAP_REASON] : 0);
            return;
        default:
            inc(ps.part->recap_counters.recap_stray);
            return;
    }
}

void RecapManager::reply_record(PState& ps, RecapSource src, const uint8_t* msg, uint32_t len) noexcept {
    Assembly& a = ps.asm_[static_cast<size_t>(src)];
    if (!a.active) { if (!a.skipping) inc(ps.part->recap_counters.recap_stray); return; } // a skipped reply was counted once, at its BEGIN
    if (a.overflow) return;
    std::vector<uint8_t>& slab = ps.slabs[static_cast<size_t>(a.slab)];
    if (static_cast<uint64_t>(a.used) + 4 + len > slab.size()) { a.overflow = true; return; }
    const uint32_t le = len; // INTENT: the record length prefix is little-endian; the agent runs on x86-64 / aarch64 LE
    std::memcpy(slab.data() + a.used, &le, 4);
    if (len != 0) std::memcpy(slab.data() + a.used + 4, msg, len);
    a.used += 4 + len;
    ++a.messages;
}

uint32_t RecapManager::section_symbol(const PState& ps, uint64_t instrument, uint16_t venue) const noexcept {
    if (ps.part->symbol_passthrough()) return instrument <= 0xFFFFFFFFull ? static_cast<uint32_t>(instrument) : 0;
    return symbols_.lookup_u64(instrument, ps.part->symbol_venue_scope() ? venue : uint16_t{0});
}

void RecapManager::reply_begin(PState& ps, RecapSource src, const BeginInfo& b) noexcept {
    RecapCounters& rc = ps.part->recap_counters;
    Assembly& a = ps.asm_[static_cast<size_t>(src)];
    if (a.active || a.skipping) { inc(rc.recap_stray); abort_assembly(ps, src); } // previous reply never ended
    if (!b.has_id || !b.has_as_of) { inc(rc.recap_stray); a.skipping = true; return; }
    uint32_t sym = 0;
    if (b.instrument != 0) {
        // INTENT: a section is matched by its instrument, whoever asked for
        // it: one published for another agent serves this one too. Skipped,
        // and not counted stray, when nobody here waits on the instrument or
        // it is older than the floor (the pipeline could not merge it).
        if (src == RecapSource::SECTIONS && ps.bulk_rid != 0)
            ps.bulk_due = ps.now + static_cast<uint64_t>(ps.cfg.timeout_ms) * 1'000'000ull; // the bulk is still arriving
        sym = section_symbol(ps, b.instrument, b.venue);
        auto it = sym != 0 ? ps.entries.find(sym) : ps.entries.end();
        if (it == ps.entries.end() || it->second.cancelled || b.as_of < it->second.floor) {
            inc(rc.recap_sections_skipped);
            a.skipping = true;
            return;
        }
        inc(rc.recap_sections);
    } else {
        auto br = ps.by_request.find(b.rid);
        if (br == ps.by_request.end()) { inc(rc.recap_stray); a.skipping = true; return; }
        sym = br->second;
    }
    if (ps.free_slabs.empty()) { inc(rc.recap_no_slab); a.skipping = true; return; } // times out, retried
    a = Assembly{};
    a.active = true;
    a.request_id = b.rid;
    a.symbol_id = sym;
    a.slab = ps.free_slabs.back();
    ps.free_slabs.pop_back();
    a.as_of = b.as_of;
    a.has_source_epoch = b.has_epoch;
    a.source_epoch = b.epoch;
}

void RecapManager::reply_end(PState& ps, RecapSource src, bool has_id, uint64_t rid) noexcept {
    RecapCounters& rc = ps.part->recap_counters;
    Assembly& a = ps.asm_[static_cast<size_t>(src)];
    if (a.skipping && !a.active) { a.skipping = false; return; }
    if (!a.active || !has_id || rid != a.request_id) { inc(rc.recap_stray); abort_assembly(ps, src); return; }
    const uint32_t sym = a.symbol_id;
    if (a.overflow) {
        inc(rc.recap_overflows);
        const uint64_t now = now_mono_ns();
        if (now - ps.last_overflow_log_ns > kOverflowLogIntervalNs) {
            ps.last_overflow_log_ns = now;
            LogRegistry::instance().warn("RECAP", "OVERFLOW", ps.part->name() + ": a reply exceeds reply_slab_bytes (" +
                                         std::to_string(ps.cfg.reply_slab_bytes) + "); raise it");
        }
        abort_assembly(ps, src);
        fail(ps, sym, RecapStatus::OVERFLOW, 0);
        return;
    }
    auto it = ps.entries.find(sym);
    if (it == ps.entries.end() || it->second.cancelled) { abort_assembly(ps, src); finish(ps, sym, true); return; }
    RecapReply r;
    r.partition = ps.part->id();
    r.slab = static_cast<uint16_t>(a.slab);
    r.symbol_id = sym;
    r.request_id = it->second.request_id != 0 ? it->second.request_id : rid; // a section nobody here asked for: its section id
    r.attempt = it->second.attempt + 1;
    r.generation = it->second.generation;
    r.status = RecapStatus::OK;
    r.as_of = a.as_of;
    r.has_source_epoch = a.has_source_epoch;
    r.source_epoch = a.source_epoch;
    r.data = ps.slabs[static_cast<size_t>(a.slab)].data();
    r.bytes = a.used;
    r.messages = a.messages;
    a = Assembly{}; // the slab now belongs to the pipeline until released
    inc(rc.recap_replies);
    push_reply(ps, r);
    finish(ps, sym, true);
}

void RecapManager::reply_reject(PState& ps, bool has_id, uint64_t rid, uint64_t reason) noexcept {
    RecapCounters& rc = ps.part->recap_counters;
    Assembly& a = ps.asm_[static_cast<size_t>(RecapSource::RESPONSE)];
    if (a.active && a.request_id == rid) abort_assembly(ps, RecapSource::RESPONSE);
    if (has_id && ps.bulk_rid != 0 && rid == ps.bulk_rid) {
        // The whole bulk is refused: each of its entries fails and retries on its own.
        inc(rc.recap_rejects);
        std::vector<uint32_t> on_bulk;
        for (const auto& [sym, e] : ps.entries) if (e.bulk && e.state == Entry::State::INFLIGHT) on_bulk.push_back(sym);
        for (const uint32_t sym : on_bulk) fail(ps, sym, RecapStatus::REJECTED, reason);
        return;
    }
    auto br = has_id ? ps.by_request.find(rid) : ps.by_request.end();
    if (br == ps.by_request.end()) { inc(rc.recap_stray); return; }
    inc(rc.recap_rejects);
    fail(ps, br->second, RecapStatus::REJECTED, reason);
}

void RecapManager::push_reply(PState& ps, const RecapReply& r) noexcept {
    // INTENT: replies keep their order per channel; once one waits in the
    // outbox, later ones queue behind it.
    bool behind = false;
    for (const auto& [ch, _] : outbox_) behind = behind || ch == ps.ch;
    if (behind || !ps.ch->replies.push(r)) outbox_.emplace_back(ps.ch, r);
}

void RecapManager::flush_outbox() noexcept {
    std::inplace_vector<RecapChannel*, 64> blocked;
    for (auto it = note_outbox_.begin(); it != note_outbox_.end();) {
        if (std::ranges::contains(blocked, it->first)) { ++it; continue; }
        if (it->first->resolved.push(it->second)) {
            --note_waiting_[it->first];
            it = note_outbox_.erase(it);
        } else {
            if (!blocked.try_push_back(it->first)) break;
            ++it;
        }
    }
    for (auto it = outbox_.begin(); it != outbox_.end();) {
        bool earlier = false;
        for (auto jt = outbox_.begin(); jt != it; ++jt) earlier = earlier || jt->first == it->first;
        if (!earlier && it->first->replies.push(it->second)) it = outbox_.erase(it);
        else ++it;
    }
}

void RecapManager::publish_gauges(PState& ps) noexcept {
    RecapCounters& rc = ps.part->recap_counters;
    rc.inflight.store(ps.inflight + ps.bulk_inflight, std::memory_order_relaxed);
    rc.queued.store(ps.entries.size() - ps.inflight - ps.bulk_inflight, std::memory_order_relaxed);
    rc.connected.store(ps.was_connected ? 1 : 0, std::memory_order_relaxed);
}

} // namespace subplane
