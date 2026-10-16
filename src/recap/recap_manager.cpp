#include "subplane/recap/recap_manager.h"

#include "subplane/core/clock.h"
#include "subplane/logging/log_registry.h"
#include "subplane/pipeline/partition.h"
#include "subplane/proto/subplane_proto_codec.h"
#include "subplane/symbol_table.h"

#include <chrono>
#include <cstring>
#include <sched.h>

namespace subplane {

namespace {
constexpr int kFragmentsPerPoll = 64;
constexpr int kCommandsPerLoop = 4096;
constexpr uint64_t kOverflowLogIntervalNs = 10'000'000'000ull;
constexpr uint64_t kStatusReasonBase = 0x100; // RecapReply.reason for a Status: base + code
constexpr std::string_view kServiceName = "subplaned";

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
        // INTENT: SPP section 5: Hello is the first message on every
        // connection, and the peer's state is per connection. A reconnect may
        // be a different (older or newer) service, so its Hello is forgotten.
        const bool conn = ps->transport->connected();
        if (conn != ps->was_connected) {
            ps->was_connected = conn;
            ps->hello_sent = false;
            ps->peer = Peer{};
            if (ps->asm_.active || ps->asm_.skipping) abort_assembly(*ps); // in-flight requests time out and retry
        }
        bool hold_requests = false;
        if (conn && !ps->hello_sent) {
            ps->hello_sent = send_hello(*ps);
            hold_requests = !ps->hello_sent; // back-pressured: the Hello goes first next loop
        }
        work += ps->transport->poll(*ps, kFragmentsPerPoll);
        service_timeouts(*ps);
        if (!hold_requests) send_ready(*ps);
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
        r.attempt = it->second.attempt + 1;
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

void RecapManager::PState::on_fragment(const uint8_t* data, uint32_t len) noexcept {
    if (cfg.reply == RecapReplyMode::SPP) {
        // INTENT: a frame is u32-length-prefixed SPP messages back to back. A
        // bad length ends the frame: nothing after it can be trusted.
        uint32_t at = 0;
        while (at < len) {
            uint32_t mlen = 0;
            if (len - at < 4) { inc(part->recap_counters.recap_stray); return; }
            std::memcpy(&mlen, data + at, 4);
            at += 4;
            if (mlen > len - at) { inc(part->recap_counters.recap_stray); return; }
            mgr->on_spp_message(*this, data + at, mlen);
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

void RecapManager::on_spp_message(PState& ps, const uint8_t* msg, uint32_t len) noexcept {
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
            reply_begin(ps, true, d.CorrelationId(), true, d.AsOf(), d.SourceEpoch() != 0, d.SourceEpoch());
            return;
        }
        case sp_proto::RecapRecordDecoder::kTemplateId: {
            sp_proto::RecapRecordDecoder d;
            if (!d.wrap(msg, len)) break;
            const Assembly& a = ps.asm_;
            if (a.active && d.CorrelationId() != a.request_id) { inc(rc.recap_stray); return; } // replies are never interleaved
            reply_record(ps, d.Payload_data(), static_cast<uint32_t>(d.Payload_size()));
            return;
        }
        case sp_proto::RecapEndDecoder::kTemplateId: {
            sp_proto::RecapEndDecoder d;
            if (!d.wrap(msg, len)) break;
            // INTENT: RecordCount guards against a reply that lost records on
            // the way; a short snapshot would be merged as if complete. The
            // request then times out and is retried.
            const Assembly& a = ps.asm_;
            if (a.active && d.CorrelationId() == a.request_id && !a.overflow && d.RecordCount() != a.messages) {
                inc(rc.recap_stray);
                abort_assembly(ps);
                return;
            }
            reply_end(ps, true, d.CorrelationId());
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
            if (ps.by_request.count(d.CorrelationId()) != 0 && code != static_cast<uint16_t>(sp_proto::StatusCode::Ok)) {
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
    if (!ps.part->read_recap_fields(msg, len, f)) { reply_record(ps, msg, len); return; }
    const bool has_id = f.has[RECAP_REQUEST_ID];
    const uint64_t rid = f.value[RECAP_REQUEST_ID];
    switch (f.marker) {
        case RecapMarker::BEGIN:
            reply_begin(ps, has_id, rid, f.has[RECAP_AS_OF], f.value[RECAP_AS_OF], f.has[RECAP_SOURCE_EPOCH], f.value[RECAP_SOURCE_EPOCH]);
            return;
        case RecapMarker::END:
            reply_end(ps, has_id, rid);
            return;
        case RecapMarker::REJECT:
            reply_reject(ps, has_id, rid, f.has[RECAP_REASON] ? f.value[RECAP_REASON] : 0);
            return;
        default:
            inc(ps.part->recap_counters.recap_stray);
            return;
    }
}

void RecapManager::reply_record(PState& ps, const uint8_t* msg, uint32_t len) noexcept {
    Assembly& a = ps.asm_;
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

void RecapManager::reply_begin(PState& ps, bool has_id, uint64_t rid, bool has_as_of, uint64_t as_of, bool has_epoch, uint64_t epoch) noexcept {
    RecapCounters& rc = ps.part->recap_counters;
    Assembly& a = ps.asm_;
    if (a.active || a.skipping) { inc(rc.recap_stray); abort_assembly(ps); } // previous reply never ended
    auto br = has_id ? ps.by_request.find(rid) : ps.by_request.end();
    if (br == ps.by_request.end() || !has_as_of) { inc(rc.recap_stray); a.skipping = true; return; }
    if (ps.free_slabs.empty()) { inc(rc.recap_no_slab); a.skipping = true; return; } // times out, retried
    a = Assembly{};
    a.active = true;
    a.request_id = rid;
    a.symbol_id = br->second;
    a.slab = ps.free_slabs.back();
    ps.free_slabs.pop_back();
    a.as_of = as_of;
    a.has_source_epoch = has_epoch;
    a.source_epoch = epoch;
}

void RecapManager::reply_end(PState& ps, bool has_id, uint64_t rid) noexcept {
    RecapCounters& rc = ps.part->recap_counters;
    Assembly& a = ps.asm_;
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
    r.attempt = it->second.attempt + 1;
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
    Assembly& a = ps.asm_;
    if (a.active && a.request_id == rid) abort_assembly(ps);
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
