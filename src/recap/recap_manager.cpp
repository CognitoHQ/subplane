#include "subplane/recap/recap_manager.h"

#include "subplane/core/clock.h"
#include "subplane/logging/log_registry.h"
#include "subplane/pipeline/partition.h"
#include "subplane/symbol_table.h"

#include <chrono>
#include <cstring>
#include <sched.h>

namespace subplane {

namespace {
constexpr int kFragmentsPerPoll = 64;
constexpr int kCommandsPerLoop = 4096;
constexpr uint64_t kOverflowLogIntervalNs = 10'000'000'000ull;
} // namespace

RecapManager::RecapManager(uint64_t epoch, const SymbolTable& symbols)
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

bool RecapManager::open(std::string& err) {
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
    if (running_ || parts_.empty()) return;
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
        work += ps->transport->poll(*ps, kFragmentsPerPoll);
        service_timeouts(*ps);
        send_ready(*ps);
        publish_gauges(*ps);
    }
    flush_outbox();
    return work;
}

void RecapManager::handle_command(PState& ps, const RecapCommand& c) noexcept {
    RecapCounters& rc = ps.part->recap_counters;
    auto it = ps.entries.find(c.symbol_id);
    if (c.op == RecapCommand::Op::REQUEST) {
        if (it != ps.entries.end()) {
            it->second.cancelled = false;
            inc(rc.recap_coalesced);
            return;
        }
        ps.entries.emplace(c.symbol_id, Entry{});
        ps.fifo.push_back(c.symbol_id);
        return;
    }
    // CANCEL: no session waits on the symbol any more.
    if (it == ps.entries.end()) return;
    inc(rc.recap_cancelled);
    if (it->second.state == Entry::State::INFLIGHT) it->second.cancelled = true; // ends with its reply or timeout
    else ps.entries.erase(it);                                                  // fifo skips it lazily
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
        if (it == ps.entries.end() || it->second.state != Entry::State::QUEUED) { ps.fifo.pop_front(); continue; }

        RecapRequestWire w;
        w.request_id = next_request_id_;
        w.agent_id = epoch_;
        if (ps.part->symbol_passthrough()) {
            w.instrument = sym;
        } else {
            w.instrument = symbols_.key_u64(sym);
            if (ps.part->symbol_venue_scope()) w.venue = symbols_.venue_u64(sym);
        }
        w.last_seq_seen = ps.part->last_seq_pub.load(std::memory_order_relaxed);
        const RecapSendStatus st = ps.transport->send(w);
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
        e.request_id = w.request_id;
        e.due_ns = ps.now + static_cast<uint64_t>(ps.cfg.timeout_ms) * 1'000'000ull;
        ps.by_request[w.request_id] = sym;
        ++ps.inflight;
        inc(rc.recap_requests);
    }
}

void RecapManager::service_timeouts(PState& ps) noexcept {
    RecapCounters& rc = ps.part->recap_counters;
    // Collected first: fail() and finish() change the map.
    uint32_t expired[64];
    for (;;) {
        int n = 0;
        for (auto& [sym, e] : ps.entries) {
            if (n == 64) break;
            if (e.state == Entry::State::INFLIGHT && ps.now >= e.due_ns) expired[n++] = sym;
            else if (e.state == Entry::State::BACKOFF && ps.now >= e.due_ns) { e.state = Entry::State::QUEUED; ps.fifo.push_back(sym); }
        }
        for (int i = 0; i < n; ++i) {
            const Entry& e = ps.entries.at(expired[i]);
            if (ps.asm_.active && ps.asm_.request_id == e.request_id) abort_assembly(ps);
            inc(rc.recap_timeouts);
            fail(ps, expired[i], RecapStatus::TIMEOUT, 0);
        }
        if (n < 64) break;
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
    if (e.state == Entry::State::INFLIGHT) {
        --ps.inflight;
        ps.by_request.erase(e.request_id);
    }
    // INTENT: retry for as long as a session waits (no attempt limit); the
    // pipeline cancels when the last waiter leaves, and every failure was
    // already reported so apps see RECAP_FAILED each time.
    if (success || e.cancelled) {
        ps.entries.erase(it);
        return;
    }
    ++e.attempt;
    e.state = Entry::State::BACKOFF;
    e.due_ns = ps.now + backoff_ns(ps, e.attempt);
}

void RecapManager::abort_assembly(PState& ps) noexcept {
    if (ps.asm_.slab >= 0) ps.free_slabs.push_back(static_cast<uint16_t>(ps.asm_.slab));
    ps.asm_ = Assembly{};
}

void RecapManager::PState::on_fragment(const uint8_t* data, uint32_t len) noexcept {
    const PacketView pkt{data, len, 0};
    FrameInfo info;
    MessageIter it;
    const Framer& fr = part->framer();
    if (!fr.parse(pkt, info, it)) { inc(part->recap_counters.recap_stray); return; }
    const uint8_t* msg = nullptr;
    uint32_t mlen = 0;
    for (;;) {
        const NextStatus st = fr.next(it, msg, mlen);
        if (st == NextStatus::OK) { mgr->on_message(*this, msg, mlen); continue; }
        if (st != NextStatus::END) inc(part->recap_counters.recap_stray);
        break;
    }
}

void RecapManager::on_message(PState& ps, const uint8_t* msg, uint32_t len) noexcept {
    RecapCounters& rc = ps.part->recap_counters;
    Assembly& a = ps.asm_;
    RecapMarkerFields f;
    if (!ps.part->read_recap_fields(msg, len, f)) {
        if (!a.active) { if (!a.skipping) inc(rc.recap_stray); return; } // a skipped reply was counted once, at its BEGIN
        if (a.overflow) return;
        std::vector<uint8_t>& slab = ps.slabs[static_cast<size_t>(a.slab)];
        if (static_cast<uint64_t>(a.used) + 4 + len > slab.size()) { a.overflow = true; return; }
        const uint32_t le = len; // INTENT: the record length prefix is little-endian; the agent runs on x86-64 / aarch64 LE
        std::memcpy(slab.data() + a.used, &le, 4);
        std::memcpy(slab.data() + a.used + 4, msg, len);
        a.used += 4 + len;
        ++a.messages;
        return;
    }
    const bool has_id = f.has[RECAP_REQUEST_ID];
    const uint64_t rid = f.value[RECAP_REQUEST_ID];
    switch (f.marker) {
        case RecapMarker::BEGIN: {
            if (a.active || a.skipping) { inc(rc.recap_stray); abort_assembly(ps); } // previous reply never ended
            auto br = has_id ? ps.by_request.find(rid) : ps.by_request.end();
            if (br == ps.by_request.end() || !f.has[RECAP_AS_OF]) { inc(rc.recap_stray); a.skipping = true; return; }
            if (ps.free_slabs.empty()) { inc(rc.recap_no_slab); a.skipping = true; return; } // times out, retried
            a = Assembly{};
            a.active = true;
            a.request_id = rid;
            a.symbol_id = br->second;
            a.slab = ps.free_slabs.back();
            ps.free_slabs.pop_back();
            a.as_of = f.value[RECAP_AS_OF];
            a.has_source_epoch = f.has[RECAP_SOURCE_EPOCH];
            a.source_epoch = f.value[RECAP_SOURCE_EPOCH];
            return;
        }
        case RecapMarker::END: {
            if (a.skipping && !a.active) { a.skipping = false; return; }
            if (!a.active || !has_id || rid != a.request_id) { inc(rc.recap_stray); abort_assembly(ps); return; }
            const uint32_t sym = a.symbol_id;
            if (a.overflow) {
                inc(rc.recap_overflows);
                const uint64_t now = now_mono_ns();
                if (now - ps.last_overflow_log_ns > kOverflowLogIntervalNs) {
                    ps.last_overflow_log_ns = now;
                    LogRegistry::instance().warn("RECAP", "OVERFLOW", ps.part->name() + ": a reply exceeds reply_slab_bytes (" +
                                                 std::to_string(ps.cfg.reply_slab_bytes) + "); raise it");
                }
                abort_assembly(ps);
                fail(ps, sym, RecapStatus::OVERFLOW, 0);
                return;
            }
            auto it = ps.entries.find(sym);
            if (it == ps.entries.end() || it->second.cancelled) { abort_assembly(ps); finish(ps, sym, true); return; }
            RecapReply r;
            r.partition = ps.part->id();
            r.slab = static_cast<uint16_t>(a.slab);
            r.symbol_id = sym;
            r.request_id = rid;
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
            return;
        }
        case RecapMarker::REJECT: {
            if (a.active && a.request_id == rid) abort_assembly(ps);
            auto br = has_id ? ps.by_request.find(rid) : ps.by_request.end();
            if (br == ps.by_request.end()) { inc(rc.recap_stray); return; }
            inc(rc.recap_rejects);
            fail(ps, br->second, RecapStatus::REJECTED, f.has[RECAP_REASON] ? f.value[RECAP_REASON] : 0);
            return;
        }
        default:
            inc(rc.recap_stray);
            return;
    }
}

void RecapManager::push_reply(PState& ps, const RecapReply& r) noexcept {
    // INTENT: replies keep their order per channel; once one waits in the
    // outbox, later ones queue behind it.
    bool behind = false;
    for (const auto& [ch, _] : outbox_) behind = behind || ch == ps.ch;
    if (behind || !ps.ch->replies.push(r)) outbox_.emplace_back(ps.ch, r);
}

void RecapManager::flush_outbox() noexcept {
    for (auto it = outbox_.begin(); it != outbox_.end();) {
        bool earlier = false;
        for (auto jt = outbox_.begin(); jt != it; ++jt) earlier = earlier || jt->first == it->first;
        if (!earlier && it->first->replies.push(it->second)) it = outbox_.erase(it);
        else ++it;
    }
}

void RecapManager::publish_gauges(PState& ps) noexcept {
    RecapCounters& rc = ps.part->recap_counters;
    rc.inflight.store(ps.inflight, std::memory_order_relaxed);
    rc.queued.store(ps.entries.size() - ps.inflight, std::memory_order_relaxed);
}

} // namespace subplane
