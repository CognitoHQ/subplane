#include "subplane/pipeline/pipeline.h"

#include "subplane/core/clock.h"
#include "subplane/shm/control_events.h"

#include <algorithm>
#include <cstring>
#include <pthread.h>
#include <sched.h>

namespace subplane {

namespace {
constexpr uint64_t kRingHeartbeatNs = 1'000'000; // 1 ms
}

// The transport's reasons are copied into the app-facing event unchanged.
static_assert(static_cast<uint32_t>(TransportReason::SOURCE_LOST) == static_cast<uint32_t>(TransportStateReason::SOURCE_LOST));
static_assert(static_cast<uint32_t>(TransportReason::LOCAL_FAILURE) == static_cast<uint32_t>(TransportStateReason::LOCAL_FAILURE));
static_assert(static_cast<uint32_t>(TransportReason::PUBLISHER_RESTART) == static_cast<uint32_t>(TransportStateReason::PUBLISHER_RESTART));

Pipeline::Pipeline(std::string name, uint64_t epoch, SymbolTable& symbols, Lvc& lvc)
    : name_(std::move(name)), epoch_(epoch), resolver_(symbols), lvc_(lvc) {
    name_buf_.reserve(64);
}

Pipeline::~Pipeline() {
    stop();
}

void Pipeline::add_partition(Partition* p) {
    partitions_.push_back(p);
    partition_mask_ |= (1ull << p->id());
    by_id_[p->id()] = p;
    if (p->has_recap()) {
        auto st = std::make_unique<RecapState>();
        st->init(p->recap_config());
        p->recap = st.get();
        recap_states_.push_back(std::move(st));
        recap_out_cap_ += 4ull * p->recap_config().max_pending + 64;
        release_out_cap_ += p->recap_config().reply_slabs + 16ull;
        recap_out_.reserve(recap_out_cap_);
        release_out_.reserve(release_out_cap_);
    }
}

bool Pipeline::start(int cpu, std::string& err) {
    if (running_.load()) {
        err = "pipeline " + name_ + " already running";
        return false;
    }
    stop_requested_.store(false);
    running_.store(true);
    thread_ = std::thread([this] { loop(); });
    if (cpu >= 0) {
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(static_cast<unsigned>(cpu), &set);
        if (pthread_setaffinity_np(thread_.native_handle(), sizeof(set), &set) != 0) {
            err = "pipeline " + name_ + ": could not pin to cpu " + std::to_string(cpu) + " (continuing unpinned)";
            // not fatal
        }
    }
    return true;
}

void Pipeline::stop() {
    if (!running_.load()) return;
    stop_requested_.store(true, std::memory_order_release);
    if (thread_.joinable()) thread_.join();
    running_.store(false);
}

bool Pipeline::post(const PipelineCommand& c) noexcept {
    if (!commands_.push(c)) return false;
    ++posted_;
    return true;
}

bool Pipeline::wait_applied(uint64_t timeout_ns) const noexcept {
    const uint64_t start = now_mono_ns();
    while (applied_.load(std::memory_order_acquire) < posted_) {
        if (now_mono_ns() - start > timeout_ns) return false;
        sched_yield();
    }
    return true;
}

void Pipeline::loop() noexcept {
    while (!stop_requested_.load(std::memory_order_acquire)) {
        run_once();
    }
    // Drain remaining commands so REMOVE/STOP acks are honoured on shutdown.
    apply_commands(now_ns());
}

int Pipeline::run_once() noexcept {
    int handled = 0;
    const uint64_t now = now_ns();
    // INTENT: apply session commands before polling. A session posted before
    // the first packet (every static YAML session at startup) must see that
    // packet; applying after the poll lost the first few thousand records of
    // a fast TCP stream to "no session attached yet".
    apply_commands(now);
    for (Partition* pp : partitions_) {
        Partition& p = *pp;
        if (!p.has_transport()) continue;
        Transport& t = p.transport();
        const int n = t.poll(batch_);
        // INTENT: state changes first, so TRANSPORT_UP precedes the records of
        // the batch that brought the source back (and DOWN precedes nothing).
        if (t.state_change_count() != 0) apply_transport_changes(p, t);
        if (n < 0) {
            inc(p.counters.transport_errors);
            continue;
        }
        for (int i = 0; i < n; ++i) {
            process_packet(p, batch_.pkts[i]);
        }
        handled += n;
    }
    // INTENT: take the clock AFTER polling. Packets stamped during this
    // iteration carry receive times later than a pre-poll `now`, and the
    // unsigned subtraction in service_timeouts would wrap and degrade a live
    // partition.
    const uint64_t after = handled ? now_ns() : now;
    service_timeouts(after);
    apply_commands(after);
    if (!recap_states_.empty()) apply_recaps(after);
    heartbeat_rings(after);
    loops_.store(loops_.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
    return handled;
}

void Pipeline::process_packet(Partition& p, const PacketView& pkt) noexcept {
    inc(p.counters.packets);
    inc(p.counters.bytes, pkt.len);
    p.last_rx_ns = pkt.recv_ts_ns;
    if (p.state != PartitionState::ACTIVE) {
        const bool was_degraded = p.state == PartitionState::DEGRADED;
        p.state = PartitionState::ACTIVE;
        publish_partition_state(p);
        if (was_degraded) {
            PartitionStateEvent ev{};
            ev.partition_id = p.id();
            ev.reason = static_cast<uint32_t>(PartitionReason::NONE);
            ev.last_seq = p.last_seq;
            ev.changed_ns = pkt.recv_ts_ns;
            emit_control(p, ControlCode::PARTITION_RECOVERED, &ev, sizeof(ev), pkt.recv_ts_ns);
        }
    }

    FrameInfo info;
    MessageIter it;
    const Framer& fr = p.framer();
    if (!fr.parse(pkt, info, it)) {
        inc(p.counters.malformed);
        return;
    }
    if (info.has_seq && !check_sequence(p, info.seq, pkt.recv_ts_ns)) {
        return; // duplicate packet
    }

    const uint8_t* msg = nullptr;
    uint32_t len = 0;
    for (;;) {
        const NextStatus st = fr.next(it, msg, len);
        if (st == NextStatus::OK) {
            process_message(p, msg, len, pkt.recv_ts_ns, info);
            continue;
        }
        if (st == NextStatus::END) break;
        switch (st) {
            case NextStatus::TRUNCATED:   inc(p.counters.truncated); break;
            case NextStatus::UNDECODABLE: inc(p.counters.undecodable_tail); break;
            case NextStatus::MALFORMED:   inc(p.counters.malformed); break;
            default: break;
        }
        break;
    }

    const uint64_t done = now_ns();
    hist_recv_to_publish_.record(done > pkt.recv_ts_ns ? done - pkt.recv_ts_ns : 0);
}

void Pipeline::process_message(Partition& p, const uint8_t* msg, uint32_t len, uint64_t recv_ts,
                               const FrameInfo& info) noexcept {
    TickHeader h{};
    h.ingest_ts = recv_ts;
    h.partition_id = p.id();
    h.payload_len = len;

    const DecodeResult r = p.decode(msg, len, h, resolver_);
    switch (r.status) {
        case DecodeStatus::OK: break;
        case DecodeStatus::UNKNOWN_TEMPLATE:
            inc(p.counters.unknown_templates);
            p.counters.last_unknown_template.store(h.template_id, std::memory_order_relaxed);
            return;
        case DecodeStatus::SCHEMA_MISMATCH:
            inc(p.counters.schema_mismatch);
            return;
        case DecodeStatus::TRUNCATED:
        case DecodeStatus::MALFORMED:
            inc(p.counters.decode_errors);
            return;
    }
    inc(p.counters.messages);

    if (const uint32_t nid = resolver_.take_new_id(); nid != 0) {
        emit_symbol_mapped(nid, recv_ts);
    }
    // INTENT: recap markers belong to snapshot-service replies, which reach the
    // agent through the recap component, not the live stream. One on the live
    // stream is counted and dropped: apps get the agent's own recap events,
    // never the publisher's markers.
    if (r.marker != RecapMarker::NONE) {
        inc(p.counters.recap_markers);
        return;
    }

    if (p.framer().seq_source() == SeqSource::MESSAGE_FIELD) {
        if (!check_sequence(p, h.sequence, recv_ts)) return; // duplicate message
    } else if (info.has_seq) {
        h.sequence = info.seq;
    }

    if (h.kind == static_cast<uint8_t>(Kind::DATA)) {
        if (p.lvc_enabled) lvc_.update(h, msg, len);
        publish_data(p, h, msg, len);
    } else {
        inc(p.counters.admin);
        const bool seq_reset = static_cast<ControlCode>(h.msg_type) == ControlCode::BUS_SEQ_RESET;
        // INTENT: the bus restarted its sequence. Accept whatever comes
        // next rather than flagging a gap against the old stream.
        if (seq_reset) p.expected_seq = 0;
        publish_admin(p, h, msg, len);
        // After the reset itself, so a recap session sees BUS_SEQ_RESET, then
        // SYMBOL_STALE. The floors wait for the new stream's first sequence.
        if (seq_reset && p.recap != nullptr) {
            recap_restart(p, SymbolStaleReason::SEQUENCE_RESET, recv_ts);
            p.recap->set_awaiting_seq(true);
        }
    }
}

bool Pipeline::check_sequence(Partition& p, uint64_t seq, uint64_t now) noexcept {
    if (p.expected_seq == 0) {
        if (p.recap != nullptr && (p.recap->pending() != 0 || p.recap->awaiting_seq())) recap_rebase(p, seq);
        p.expected_seq = seq + 1;
        p.last_seq = seq;
        p.last_seq_pub.store(seq, std::memory_order_relaxed);
        return true;
    }
    if (seq == p.expected_seq) {
        p.expected_seq = seq + 1;
        p.last_seq = seq;
        p.last_seq_pub.store(seq, std::memory_order_relaxed);
        return true;
    }
    if (seq > p.expected_seq) {
        inc(p.counters.gaps);
        GapEvent ev{};
        ev.partition_id = p.id();
        ev.expected_seq = p.expected_seq;
        ev.received_seq = seq;
        ev.detected_ns = now;
        emit_control(p, ControlCode::GAP_DETECTED, &ev, sizeof(ev), now);
        // INTENT: every recap session's view now has a hole before `seq`, for
        // any of its symbols (the sequence is per stream, not per symbol). All
        // of them become pending, and only a snapshot as of seq - 1 or later
        // closes the hole. Before last_seq moves: the events carry the old one.
        if (p.recap != nullptr) {
            recap_restart(p, SymbolStaleReason::GAP, now);
            recap_rebase(p, seq);
        }
        p.expected_seq = seq + 1;
        p.last_seq = seq;
        p.last_seq_pub.store(seq, std::memory_order_relaxed);
        return true;
    }
    if (p.expected_seq - seq > p.dup_window) {
        // INTENT: a publisher that restarted its sequence without a SEQ_RESET
        // message must not be black-holed as an endless duplicate. A backward
        // jump beyond dup_window is treated as a reset: counted, reported
        // in-band as a GAP with received < expected, and resynced.
        inc(p.counters.seq_resets);
        GapEvent ev{};
        ev.partition_id = p.id();
        ev.expected_seq = p.expected_seq;
        ev.received_seq = seq;
        ev.detected_ns = now;
        emit_control(p, ControlCode::GAP_DETECTED, &ev, sizeof(ev), now);
        if (p.recap != nullptr) {
            recap_restart(p, SymbolStaleReason::SEQUENCE_RESET, now);
            recap_rebase(p, seq);
        }
        p.expected_seq = seq + 1;
        p.last_seq = seq;
        p.last_seq_pub.store(seq, std::memory_order_relaxed);
        return true;
    }
    inc(p.counters.dups);
    return false;
}

void Pipeline::publish_data(Partition& p, const TickHeader& h, const uint8_t* bytes, uint32_t len) noexcept {
    // INTENT: one load and a branch when nothing is pending (the common case);
    // a pending symbol's message is held once for its waiters, and every
    // other session still gets it now (recap never delays a live session).
    const RecapPending* pending = nullptr;
    if (p.recap != nullptr && p.recap->pending() != 0) [[unlikely]] pending = recap_hold(p, h, bytes, len);
    for (Session* sp : p.sessions) {
        Session& s = *sp;
        if (pending != nullptr && s.recap && pending->waiters.test(s.recap_slot)) continue;
        const FilterTable* f = s.filter.load(std::memory_order_acquire);
        if (f == nullptr || !f->accept(h)) {
            inc(s.counters.filtered);
            continue;
        }
        if (s.ring.publish(h, bytes, len)) inc(s.counters.published);
        else inc(s.counters.publish_failed);
    }
}

void Pipeline::publish_admin(Partition& p, const TickHeader& h, const uint8_t* bytes, uint32_t len) noexcept {
    for (Session* sp : p.sessions) {
        Session& s = *sp;
        const FilterTable* f = s.filter.load(std::memory_order_acquire);
        if (f == nullptr || !f->accept(h)) continue;
        if (s.ring.publish(h, bytes, len)) inc(s.counters.published);
        else inc(s.counters.publish_failed);
    }
}

void Pipeline::emit_control(Partition& p, ControlCode code, const void* body, uint32_t len, uint64_t now) noexcept {
    for (Session* sp : p.sessions) {
        Session& s = *sp;
        const FilterTable* f = s.filter.load(std::memory_order_acquire);
        if (f == nullptr || !f->agent_control) continue;
        if (!s.ring.publish_control(code, p.id(), now, body, len)) inc(s.counters.publish_failed);
    }
}

void Pipeline::emit_control_all(ControlCode code, const void* body, uint32_t len, uint64_t now) noexcept {
    for (Session* sp : sessions_) {
        Session& s = *sp;
        const FilterTable* f = s.filter.load(std::memory_order_acquire);
        if (f == nullptr || !f->agent_control) continue;
        if (!s.ring.publish_control(code, 0, now, body, len)) inc(s.counters.publish_failed);
    }
}

void Pipeline::apply_transport_changes(Partition& p, Transport& t) noexcept {
    const uint64_t now = now_ns();
    for (int i = 0; i < t.state_change_count(); ++i) {
        const TransportStateChange& c = t.state_change(i);
        const bool up = c.state == TransportState::UP;
        inc(up ? p.counters.transport_up : p.counters.transport_down);
        TransportStateEvent ev{};
        ev.partition_id = p.id();
        ev.reason = static_cast<uint32_t>(c.reason);
        ev.last_seq = p.last_seq;
        ev.changed_ns = now;
        emit_control(p, up ? ControlCode::TRANSPORT_UP : ControlCode::TRANSPORT_DOWN, &ev, sizeof(ev), now);
        // INTENT: a new publisher instance is a new stream: its sequences say
        // nothing about the old one's, so the next sequence is accepted as a
        // fresh start (as after BUS_SEQ_RESET), on every partition. Comparing
        // them would report a misleading reset, or drop up to dup_window of
        // the new publisher's messages as duplicates when it starts just
        // below the old one. On a recap partition the floors wait for that
        // first sequence. A transport going down and up with the same
        // publisher is no trigger: anything missed shows as a gap.
        if (up && c.reason == TransportReason::PUBLISHER_RESTART) {
            p.expected_seq = 0;
            if (p.recap != nullptr) {
                recap_restart(p, SymbolStaleReason::PUBLISHER_RESTART, now);
                p.recap->set_awaiting_seq(true);
            }
        }
    }
    t.clear_state_changes();
}

void Pipeline::emit_symbol_mapped(uint32_t id, uint64_t now) noexcept {
    SymbolMappedEvent ev{};
    ev.symbol_id = id;
    name_buf_.clear();
    if (resolver_.table().resolve(id, name_buf_)) {
        const size_t n = name_buf_.size() < sizeof(ev.name) ? name_buf_.size() : sizeof(ev.name);
        std::memcpy(ev.name, name_buf_.data(), n);
        ev.name_len = static_cast<uint8_t>(n);
    }
    emit_control_all(ControlCode::SYMBOL_MAPPED, &ev, sizeof(ev), now);
}

void Pipeline::service_timeouts(uint64_t now) noexcept {
    for (Partition* pp : partitions_) {
        Partition& p = *pp;
        if (p.state != PartitionState::ACTIVE) continue;
        if (now > p.last_rx_ns && now - p.last_rx_ns > p.heartbeat_timeout_ns()) {
            p.state = PartitionState::DEGRADED;
            inc(p.counters.degraded_events);
            publish_partition_state(p);
            PartitionStateEvent ev{};
            ev.partition_id = p.id();
            ev.reason = static_cast<uint32_t>(PartitionReason::HEARTBEAT_TIMEOUT);
            ev.last_seq = p.last_seq;
            ev.changed_ns = now;
            emit_control(p, ControlCode::PARTITION_DEGRADED, &ev, sizeof(ev), now);
        }
    }
}

void Pipeline::publish_partition_state(Partition& p) noexcept {
    p.state_pub.store(static_cast<uint8_t>(p.state), std::memory_order_relaxed);
    p.last_rx_pub.store(p.last_rx_ns, std::memory_order_relaxed);
}

void Pipeline::apply_commands(uint64_t now) noexcept {
    PipelineCommand c;
    while (commands_.pop(c)) {
        switch (c.type) {
            case PipelineCommand::Type::ADD_SESSION: {
                Session& s = *c.session;
                bool added = false;
                for (Partition* p : partitions_) {
                    if ((s.partition_mask >> p->id()) & 1u) {
                        added |= p->attach(&s);
                    }
                }
                const bool known = std::ranges::contains(sessions_, &s);
                if (!known) sessions_.try_push_back(&s);   // full: as before, no group-wide events
                if (s.recap && !known) {
                    // INTENT: a slot per recap session, not its array index:
                    // Partition::detach reorders sessions[]. There are as many
                    // slots as a pipeline has sessions, so one is always free.
                    for (uint32_t i = 0; i < RecapWaiters::kSlots; ++i) {
                        if (!recap_slots_.test(i)) { recap_slots_.set(i); s.recap_slot = static_cast<uint16_t>(i); break; }
                    }
                }
                if (added || !known) {
                    AgentRestartedEvent ev{};
                    ev.epoch = epoch_;
                    const FilterTable* f = s.filter.load(std::memory_order_acquire);
                    if (f == nullptr || f->agent_control) {
                        s.ring.publish_control(ControlCode::AGENT_RESTARTED, 0, now, &ev, sizeof(ev));
                    }
                }
                // INTENT: stamp liveness now. The periodic heartbeat is gated to
                // ~1 kHz, and a reader attaching inside that window would see a
                // zero heartbeat and declare the writer gone.
                s.ring.heartbeat(now);
                // After AGENT_RESTARTED: a recap session's RECAP_PENDING events follow it.
                if (s.recap && !known) recap_track(s, nullptr, s.filter.load(std::memory_order_acquire), now);
                s.attached.store(true, std::memory_order_release);
                break;
            }
            case PipelineCommand::Type::REMOVE_SESSION: {
                Session& s = *c.session;
                if (s.recap) recap_drop_session(s);
                for (Partition* p : partitions_) p->detach(&s);
                if (const auto it = std::ranges::find(sessions_, &s); it != sessions_.end()) {
                    *it = sessions_.back();
                    sessions_.pop_back();
                }
                SessionClosingEvent ev{};
                ev.session_id = s.id;
                ev.reason = c.generation; // caller passes the reason code here
                const FilterTable* f = s.filter.load(std::memory_order_acquire);
                if (f == nullptr || f->agent_control) {
                    s.ring.publish_control(ControlCode::SESSION_CLOSING, 0, now, &ev, sizeof(ev));
                }
                s.attached.store(false, std::memory_order_release);
                break;
            }
            case PipelineCommand::Type::SWAP_FILTER: {
                Session& s = *c.session;
                const FilterTable* before = s.filter.load(std::memory_order_acquire);
                s.filter.store(c.filter, std::memory_order_release);
                FilterReloadedEvent ev{};
                ev.session_id = s.id;
                ev.generation = c.generation;
                if (c.filter == nullptr || c.filter->agent_control) {
                    s.ring.publish_control(ControlCode::FILTER_RELOADED, 0, now, &ev, sizeof(ev));
                }
                if (s.recap) recap_track(s, before, c.filter, now);
                s.applied_generation.store(c.generation, std::memory_order_release);
                break;
            }
            case PipelineCommand::Type::STOP:
                stop_requested_.store(true, std::memory_order_release);
                break;
        }
        applied_.store(applied_.load(std::memory_order_relaxed) + 1, std::memory_order_release);
    }
}

void Pipeline::heartbeat_rings(uint64_t now) noexcept {
    if (now - last_heartbeat_ns_ < kRingHeartbeatNs) return;
    last_heartbeat_ns_ = now;
    for (Session* sp : sessions_) sp->ring.heartbeat(now);
}

// ---------------------------------------------------------------------------
// Recap: hold and merge (architecture.md, "Recap")
// ---------------------------------------------------------------------------

namespace {
RecapEventStatus event_status(RecapStatus st) noexcept {
    switch (st) {
        case RecapStatus::TIMEOUT:     return RecapEventStatus::TIMEOUT;
        case RecapStatus::REJECTED:    return RecapEventStatus::REJECTED;
        case RecapStatus::OVERFLOW:    return RecapEventStatus::REPLY_TOO_LARGE;
        case RecapStatus::SEND_FAILED: return RecapEventStatus::SEND_FAILED;
        default:                       return RecapEventStatus::OK;
    }
}
} // namespace

const RecapPending* Pipeline::recap_hold(Partition& p, const TickHeader& h, const uint8_t* bytes, uint32_t len) noexcept {
    RecapState& st = *p.recap;
    RecapPending* e = st.find(h.symbol_id);
    if (e == nullptr) return nullptr;
    // INTENT: held whatever the waiters' filters say; they are applied at the
    // merge, so a waiter that joins later with another filter still gets every
    // message it accepts.
    if (st.hold(*e, h, bytes, len)) {
        e->overflowed = false;
        inc(p.recap_counters.recap_held);
        return e;
    }
    // INTENT: out of room. Drop this symbol's holds and raise its floor to this
    // message, so only a snapshot that already contains it can merge; a reply
    // older than that fails as too old and is asked for again. The waiters are
    // told once per overflow, not once per message.
    inc(p.recap_counters.recap_hold_overflows);
    st.free_holds(*e);
    if (e->floor < h.sequence) e->floor = h.sequence;
    if (!e->overflowed) {
        e->overflowed = true;
        RecapEvent ev{};
        ev.symbol_id = e->symbol_id;
        ev.partition_id = p.id();
        ev.status = static_cast<uint8_t>(RecapEventStatus::HOLD_OVERFLOW);
        recap_emit(p, *e, ControlCode::RECAP_FAILED, ev, h.ingest_ts);
    }
    return e;
}

void Pipeline::recap_track(Session& s, const FilterTable* before, const FilterTable* after, uint64_t now) noexcept {
    const size_t nb = before ? before->symbol_bits.size() : 0;
    const size_t na = after ? after->symbol_bits.size() : 0;
    const size_t n = nb > na ? nb : na;
    for (Partition* pp : partitions_) {
        Partition& p = *pp;
        if (p.recap == nullptr || !((s.partition_mask >> p.id()) & 1ull)) continue;
        for (size_t w = 0; w < n; ++w) {
            const uint64_t b = w < nb ? before->symbol_bits[w] : 0;
            const uint64_t a = w < na ? after->symbol_bits[w] : 0;
            for (uint64_t added = a & ~b; added != 0; added &= added - 1) {
                recap_add_waiter(p, s, static_cast<uint32_t>(w * 64 + static_cast<size_t>(__builtin_ctzll(added))), now);
            }
            for (uint64_t gone = b & ~a; gone != 0; gone &= gone - 1) {
                const uint32_t sym = static_cast<uint32_t>(w * 64 + static_cast<size_t>(__builtin_ctzll(gone)));
                if (RecapPending* e = p.recap->find(sym); e != nullptr && e->waiters.test(s.recap_slot)) recap_drop_waiter(p, *e, s);
            }
        }
    }
}

void Pipeline::recap_add_waiter(Partition& p, Session& s, uint32_t symbol_id, uint64_t now) noexcept {
    RecapState& st = *p.recap;
    RecapEvent ev{};
    ev.symbol_id = symbol_id;
    ev.partition_id = p.id();
    RecapPending* e = st.find(symbol_id);
    if (e == nullptr) {
        // INTENT: the floor is the last sequence this partition delivered:
        // the session was never owed anything before it, and every message
        // after it is held from here on.
        e = st.insert(symbol_id, p.last_seq);
        if (e == nullptr) {
            // Unreachable while SessionManager enforces max_pending; reported, not silent.
            inc(p.recap_counters.recap_hold_overflows);
            ev.status = static_cast<uint8_t>(RecapEventStatus::HOLD_OVERFLOW);
            s.ring.publish_control(ControlCode::RECAP_FAILED, p.id(), now, &ev, sizeof(ev));
            return;
        }
        recap_post(p, {RecapCommand::Op::REQUEST, p.id(), symbol_id});
    }
    if (e->waiters.test(s.recap_slot)) return;
    e->waiters.set(s.recap_slot);
    if (!s.ring.publish_control(ControlCode::RECAP_PENDING, p.id(), now, &ev, sizeof(ev))) inc(s.counters.publish_failed);
}

void Pipeline::recap_drop_waiter(Partition& p, RecapPending& e, Session& s) noexcept {
    e.waiters.clear(s.recap_slot);
    if (e.waiters.any()) return;
    if (e.parked) recap_release(p, e.reply.slab);
    recap_post(p, {RecapCommand::Op::CANCEL, p.id(), e.symbol_id});
    p.recap->erase(e);
}

void Pipeline::recap_drop_session(Session& s) noexcept {
    for (Partition* pp : partitions_) {
        Partition& p = *pp;
        if (p.recap == nullptr || p.recap->pending() == 0) continue;
        for (uint32_t i = 0; i < p.recap->capacity(); ++i) {
            RecapPending& e = p.recap->at(i);
            if (e.in_use && e.waiters.test(s.recap_slot)) recap_drop_waiter(p, e, s);
        }
    }
    recap_slots_.clear(s.recap_slot);
}

void Pipeline::recap_restart(Partition& p, SymbolStaleReason reason, uint64_t now) noexcept {
    // INTENT: the stream changed under the recaps (architecture.md, "Recap",
    // triggers). Holds and parked replies describe the stream as it was, and
    // a request in flight would be answered with a snapshot of it: the new
    // generation renews every pending request and drops any older reply.
    // Then every symbol of every recap session on the partition is stale and
    // pending, whether it was live or already waiting.
    RecapState& st = *p.recap;
    st.renew();
    inc(p.recap_counters.recap_resyncs);
    for (uint32_t i = 0; i < st.capacity() && st.pending() != 0; ++i) {
        RecapPending& e = st.at(i);
        if (!e.in_use) continue;
        st.free_holds(e);
        if (e.parked) {
            recap_release(p, e.reply.slab);
            st.clear_parked(e);
        }
        e.overflowed = false;
        recap_post(p, {RecapCommand::Op::REQUEST, p.id(), e.symbol_id});
    }
    SymbolStaleEvent ev{};
    ev.partition_id = p.id();
    ev.reason = static_cast<uint16_t>(reason);
    ev.last_seq = p.last_seq;
    ev.changed_ns = now;
    for (Session* sp : p.sessions) {
        Session& s = *sp;
        if (!s.recap) continue;
        const FilterTable* f = s.filter.load(std::memory_order_acquire);
        if (f == nullptr) continue;
        for (size_t w = 0; w < f->symbol_bits.size(); ++w) {
            for (uint64_t bits = f->symbol_bits[w]; bits != 0; bits &= bits - 1) {
                ev.symbol_id = static_cast<uint32_t>(w * 64 + static_cast<size_t>(__builtin_ctzll(bits)));
                if (f->agent_control && !s.ring.publish_control(ControlCode::SYMBOL_STALE, p.id(), now, &ev, sizeof(ev))) {
                    inc(s.counters.publish_failed);
                }
                recap_add_waiter(p, s, ev.symbol_id, now); // RECAP_PENDING when it was live
            }
        }
    }
}

void Pipeline::recap_rebase(Partition& p, uint64_t seq) noexcept {
    // INTENT: the partition's sequence (re)starts at `seq`, after a gap, a
    // reset, a restart or at the first message. Only a snapshot as of seq - 1
    // or later covers the messages from `seq` on, and nothing held before
    // `seq` is needed with one.
    RecapState& st = *p.recap;
    st.set_awaiting_seq(false);
    for (uint32_t i = 0; i < st.capacity() && st.pending() != 0; ++i) {
        RecapPending& e = st.at(i);
        if (!e.in_use) continue;
        st.free_holds(e);
        e.floor = seq != 0 ? seq - 1 : 0;
    }
}

void Pipeline::apply_recaps(uint64_t now) noexcept {
    recap_flush();
    RecapReply r;
    for (int i = 0; i < 64 && recap_.replies.pop(r); ++i) {
        Partition* p = r.partition < 64 ? by_id_[r.partition] : nullptr;
        if (p == nullptr || p->recap == nullptr) continue; // cannot happen: the manager serves this group's partitions
        recap_reply(*p, r, now);
    }
    for (Partition* pp : partitions_) {
        Partition& p = *pp;
        if (p.recap == nullptr || p.recap->parked() == 0 || p.recap->awaiting_seq()) continue;
        for (uint32_t i = 0; i < p.recap->capacity() && p.recap->parked() != 0; ++i) {
            RecapPending& e = p.recap->at(i);
            if (!e.in_use || !e.parked) continue;
            const RecapReply parked = e.reply;
            if (parked.as_of <= p.last_seq || parked.as_of < e.floor) recap_consider(p, e, parked, now);
        }
    }
}

void Pipeline::recap_reply(Partition& p, const RecapReply& r, uint64_t now) noexcept {
    const bool ok = r.status == RecapStatus::OK;
    if (r.generation != p.recap->generation()) {
        // INTENT: asked for before the stream changed (recap_restart); the
        // renewed request's reply follows. Merging this one could hide a hole.
        inc(p.recap_counters.recap_superseded);
        if (ok) recap_release(p, r.slab);
        return;
    }
    RecapPending* e = p.recap->find(r.symbol_id);
    if (e == nullptr) { // nobody waits any more (cancelled)
        if (ok) recap_release(p, r.slab);
        return;
    }
    if (!ok) {
        // The manager retries on its own; the waiters hear of every failure.
        RecapEvent ev{};
        ev.request_id = r.request_id;
        ev.symbol_id = r.symbol_id;
        ev.partition_id = p.id();
        ev.status = static_cast<uint8_t>(event_status(r.status));
        ev.attempt = r.attempt;
        recap_emit(p, *e, ControlCode::RECAP_FAILED, ev, now);
        return;
    }
    if (e->parked) { // a newer reply replaces a parked one
        recap_release(p, e->reply.slab);
        p.recap->clear_parked(*e);
    }
    recap_consider(p, *e, r, now);
}

void Pipeline::recap_consider(Partition& p, RecapPending& e, const RecapReply& r, uint64_t now) noexcept {
    if (p.recap->awaiting_seq()) {
        // The new stream's first sequence sets the floors; until then a reply
        // cannot be compared with anything.
        p.recap->set_parked(e, r);
        return;
    }
    if (r.as_of < e.floor) {
        // INTENT: messages between S and the floor reached neither the
        // snapshot nor the hold; merging would leave a hole.
        inc(p.recap_counters.recap_too_old);
        p.recap->clear_parked(e);
        recap_release(p, r.slab);
        RecapEvent ev{};
        ev.request_id = r.request_id;
        ev.symbol_id = e.symbol_id;
        ev.partition_id = p.id();
        ev.status = static_cast<uint8_t>(RecapEventStatus::TOO_OLD);
        ev.attempt = r.attempt;
        ev.as_of_seq = r.as_of;
        recap_emit(p, e, ControlCode::RECAP_FAILED, ev, now);
        recap_post(p, {RecapCommand::Op::REQUEST, p.id(), e.symbol_id});
        return;
    }
    if (r.as_of > p.last_seq) {
        // INTENT: the service is ahead of this agent. Keep holding until live
        // reaches S; the held messages up to S are then already in the snapshot.
        p.recap->set_parked(e, r);
        return;
    }
    p.recap->clear_parked(e);
    recap_merge(p, e, r, now);
}

void Pipeline::recap_merge(Partition& p, RecapPending& e, const RecapReply& r, uint64_t now) noexcept {
    const uint64_t S = r.as_of;
    // INTENT: unchecked is safe: the waiters are a subset of p.sessions, whose
    // capacity is at most merge_waiters_'s (static_assert below).
    static_assert(Partition::kMaxSessions <= RecapWaiters::kSlots);
    merge_waiters_.clear();
    for (Session* s : p.sessions) {
        if (s->recap && e.waiters.test(s->recap_slot)) {
            merge_waiters_.unchecked_push_back(s);
            merge_counts_[s->recap_slot] = 0;
        }
    }
    RecapEvent ev{};
    ev.request_id = r.request_id;
    ev.symbol_id = e.symbol_id;
    ev.partition_id = p.id();
    ev.attempt = r.attempt;
    ev.record_count = r.messages;
    ev.as_of_seq = S;
    for (Session* s : merge_waiters_) {
        if (!s->ring.publish_control(ControlCode::RECAP_BEGIN, p.id(), now, &ev, sizeof(ev))) inc(s->counters.publish_failed);
    }

    // INTENT: the reply records are delivered as snapshot-origin data at
    // sequence S. Only DATA records of the recapped symbol are delivered;
    // anything else would reach a session outside its own recap. They do not
    // touch the last-value cache, which holds live data only.
    uint32_t at = 0;
    for (uint32_t i = 0; i < r.messages; ++i) {
        if (static_cast<uint64_t>(at) + 4 > r.bytes) break;
        uint32_t len;
        std::memcpy(&len, r.data + at, 4);
        const uint8_t* msg = r.data + at + 4;
        if (static_cast<uint64_t>(at) + 4 + len > r.bytes) break;
        at += 4 + len;
        TickHeader h{};
        h.ingest_ts = now;
        h.partition_id = p.id();
        h.payload_len = len;
        const DecodeResult dr = p.decode(msg, len, h, resolver_);
        if (const uint32_t nid = resolver_.take_new_id(); nid != 0) emit_symbol_mapped(nid, now);
        if (dr.status != DecodeStatus::OK || dr.marker != RecapMarker::NONE ||
            h.kind != static_cast<uint8_t>(Kind::DATA) || h.symbol_id != e.symbol_id) {
            continue;
        }
        h.sequence = S;
        h.flags |= FLAG_SNAPSHOT_ORIGIN;
        inc(p.recap_counters.recap_records);
        for (Session* sp : merge_waiters_) {
            Session& s = *sp;
            const FilterTable* f = s.filter.load(std::memory_order_acquire);
            if (f == nullptr || !f->accept(h)) continue;
            if (s.ring.publish(h, msg, len)) { inc(s.counters.published); ++merge_counts_[s.recap_slot]; }
            else inc(s.counters.publish_failed);
        }
    }
    for (Session* sp : merge_waiters_) {
        Session& s = *sp;
        ev.record_count = merge_counts_[s.recap_slot];
        if (!s.ring.publish_control(ControlCode::RECAP_COMPLETE, p.id(), now, &ev, sizeof(ev))) inc(s.counters.publish_failed);
    }
    // Then the held live messages the snapshot does not contain, in order.
    p.recap->for_each_held(e, [&](const TickHeader& h, const uint8_t* payload, uint32_t len) {
        if (h.sequence <= S) return;
        for (Session* sp : merge_waiters_) {
            Session& s = *sp;
            const FilterTable* f = s.filter.load(std::memory_order_acquire);
            if (f == nullptr || !f->accept(h)) { inc(s.counters.filtered); continue; }
            if (s.ring.publish(h, payload, len)) inc(s.counters.published);
            else inc(s.counters.publish_failed);
        }
    });
    inc(p.recap_counters.recap_merged);
    recap_release(p, r.slab);
    p.recap->erase(e);
}

void Pipeline::recap_emit(Partition& p, const RecapPending& e, ControlCode code, const RecapEvent& ev, uint64_t now) noexcept {
    for (Session* sp : p.sessions) {
        Session& s = *sp;
        if (!s.recap || !e.waiters.test(s.recap_slot)) continue;
        if (!s.ring.publish_control(code, p.id(), now, &ev, sizeof(ev))) inc(s.counters.publish_failed);
    }
}

void Pipeline::recap_post(Partition& p, const RecapCommand& cmd) noexcept {
    RecapCommand c = cmd;
    c.generation = p.recap->generation();
    if (recap_out_.empty() && recap_.requests.push(c)) return;
    if (recap_out_.size() < recap_out_cap_) recap_out_.push_back(c);
    else inc(p.recap_counters.recap_post_dropped);
}

void Pipeline::recap_release(Partition& p, uint16_t slab) noexcept {
    const RecapRelease rel{p.id(), slab};
    if (release_out_.empty() && recap_.releases.push(rel)) return;
    if (release_out_.size() < release_out_cap_) release_out_.push_back(rel);
    else inc(p.recap_counters.recap_post_dropped);
}

void Pipeline::recap_flush() noexcept {
    size_t i = 0;
    while (i < recap_out_.size() && recap_.requests.push(recap_out_[i])) ++i;
    if (i != 0) recap_out_.erase(recap_out_.begin(), recap_out_.begin() + static_cast<std::ptrdiff_t>(i));
    i = 0;
    while (i < release_out_.size() && recap_.releases.push(release_out_[i])) ++i;
    if (i != 0) release_out_.erase(release_out_.begin(), release_out_.begin() + static_cast<std::ptrdiff_t>(i));
}

} // namespace subplane
