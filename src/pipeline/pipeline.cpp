#include "subplane/pipeline/pipeline.h"

#include "subplane/core/clock.h"
#include "subplane/shm/control_events.h"

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
        switch (static_cast<ControlCode>(h.msg_type)) {
            case ControlCode::BUS_SEQ_RESET:
                // INTENT: the bus restarted its sequence. Accept whatever comes
                // next rather than flagging a gap against the old stream.
                p.expected_seq = 0;
                break;
            case ControlCode::BUS_HEARTBEAT:
            default:
                break;
        }
        publish_admin(p, h, msg, len);
    }
}

bool Pipeline::check_sequence(Partition& p, uint64_t seq, uint64_t now) noexcept {
    if (p.expected_seq == 0) {
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
        p.expected_seq = seq + 1;
        p.last_seq = seq;
        p.last_seq_pub.store(seq, std::memory_order_relaxed);
        return true;
    }
    inc(p.counters.dups);
    return false;
}

void Pipeline::publish_data(Partition& p, const TickHeader& h, const uint8_t* bytes, uint32_t len) noexcept {
    for (uint32_t i = 0; i < p.n_sessions; ++i) {
        Session& s = *p.sessions[i];
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
    for (uint32_t i = 0; i < p.n_sessions; ++i) {
        Session& s = *p.sessions[i];
        const FilterTable* f = s.filter.load(std::memory_order_acquire);
        if (f == nullptr || !f->accept(h)) continue;
        if (s.ring.publish(h, bytes, len)) inc(s.counters.published);
        else inc(s.counters.publish_failed);
    }
}

void Pipeline::emit_control(Partition& p, ControlCode code, const void* body, uint32_t len, uint64_t now) noexcept {
    for (uint32_t i = 0; i < p.n_sessions; ++i) {
        Session& s = *p.sessions[i];
        const FilterTable* f = s.filter.load(std::memory_order_acquire);
        if (f == nullptr || !f->agent_control) continue;
        if (!s.ring.publish_control(code, p.id(), now, body, len)) inc(s.counters.publish_failed);
    }
}

void Pipeline::emit_control_all(ControlCode code, const void* body, uint32_t len, uint64_t now) noexcept {
    for (uint32_t i = 0; i < n_sessions_; ++i) {
        Session& s = *sessions_[i];
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
                bool known = false;
                for (uint32_t i = 0; i < n_sessions_; ++i) known |= sessions_[i] == &s;
                if (!known && n_sessions_ < Partition::kMaxSessions) sessions_[n_sessions_++] = &s;
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
                s.attached.store(true, std::memory_order_release);
                break;
            }
            case PipelineCommand::Type::REMOVE_SESSION: {
                Session& s = *c.session;
                for (Partition* p : partitions_) p->detach(&s);
                for (uint32_t i = 0; i < n_sessions_; ++i) {
                    if (sessions_[i] == &s) {
                        sessions_[i] = sessions_[n_sessions_ - 1];
                        sessions_[--n_sessions_] = nullptr;
                        break;
                    }
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
                s.filter.store(c.filter, std::memory_order_release);
                FilterReloadedEvent ev{};
                ev.session_id = s.id;
                ev.generation = c.generation;
                if (c.filter == nullptr || c.filter->agent_control) {
                    s.ring.publish_control(ControlCode::FILTER_RELOADED, 0, now, &ev, sizeof(ev));
                }
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
    for (uint32_t i = 0; i < n_sessions_; ++i) sessions_[i]->ring.heartbeat(now);
}

} // namespace subplane
