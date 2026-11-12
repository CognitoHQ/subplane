#pragma once

// One pipeline thread group: owns a set of partitions and drives
// poll -> frame -> decode -> filter -> publish for all sessions attached to
// those partitions. Everything in the loop is single-threaded; the cold side
// talks to it only through the SPSC command queue.

#include "subplane/core/histogram.h"
#include "subplane/core/spsc_queue.h"
#include "subplane/pipeline/lvc.h"
#include "subplane/pipeline/partition.h"
#include "subplane/pipeline/recap_state.h"
#include "subplane/pipeline/session.h"
#include "subplane/pipeline/symbol_resolver.h"
#include "subplane/recap/recap_manager.h"
#include "subplane/shm/control_events.h"

#include <array>
#include <atomic>
#include <cstdint>
#include <inplace_vector>
#include <string>
#include <memory>
#include <thread>
#include <vector>

namespace subplane {

struct PipelineCommand {
    enum class Type : uint8_t { ADD_SESSION, REMOVE_SESSION, SWAP_FILTER, STOP };
    Type               type = Type::STOP;
    Session*           session = nullptr;
    const FilterTable* filter = nullptr;
    uint32_t           generation = 0;
};

class Pipeline {
public:
    Pipeline(std::string name, uint64_t epoch, SymbolTable& symbols, Lvc& lvc);
    ~Pipeline();

    // Cold, before start(). Partitions must have distinct ids < 64.
    void add_partition(Partition* p);
    const std::vector<Partition*>& partitions() const noexcept { return partitions_; }
    uint64_t partition_mask() const noexcept { return partition_mask_; }

    bool start(int cpu, std::string& err);
    void stop();
    bool running() const noexcept { return running_.load(std::memory_order_acquire); }

    // Cold -> hot. False if the queue is full (caller retries).
    bool post(const PipelineCommand& c) noexcept;
    uint64_t commands_posted() const noexcept { return posted_; }
    uint64_t commands_applied() const noexcept { return applied_.load(std::memory_order_acquire); }
    // Cold. Spins until all posted commands are applied or timeout (false).
    bool wait_applied(uint64_t timeout_ns) const noexcept;

    // Hot (or test). One iteration: polls every partition once, applies
    // commands, services timeouts and ring heartbeats. Returns packets handled.
    int run_once() noexcept;

    const std::string& name() const noexcept { return name_; }
    const LogHistogram& hist_recv_to_publish() const noexcept { return hist_recv_to_publish_; }
    uint64_t loops() const noexcept { return loops_.load(std::memory_order_relaxed); }
    uint64_t epoch() const noexcept { return epoch_; }
    // Recap queues to and from the agent's RecapManager.
    RecapChannel& recap_channel() noexcept { return recap_; }

private:
    void loop() noexcept;
    void process_packet(Partition& p, const PacketView& pkt) noexcept;
    void tap_unit(Partition& p, const PacketView& pkt) noexcept;
    void process_message(Partition& p, const uint8_t* msg, uint32_t len, uint64_t recv_ts,
                         const FrameInfo& info) noexcept;
    // Returns false if the packet/message is a duplicate and must be dropped.
    bool check_sequence(Partition& p, uint64_t seq, uint64_t now) noexcept;
    // `stream_seq` is the partition's sequence of the record (h.sequence may be
    // the instrument's); the recap hold and floors work in stream sequences.
    void publish_data(Partition& p, const TickHeader& h, uint64_t stream_seq, const uint8_t* bytes, uint32_t len) noexcept;
    void instrument_gap(Partition& p, const TickHeader& h, uint64_t expected, uint64_t received, uint64_t now) noexcept;
    void inst_reset(Partition& p) noexcept;
    void publish_admin(Partition& p, const TickHeader& h, const uint8_t* bytes, uint32_t len) noexcept;
    void emit_control(Partition& p, ControlCode code, const void* body, uint32_t len, uint64_t now) noexcept;
    void emit_control_all(ControlCode code, const void* body, uint32_t len, uint64_t now) noexcept;
    void emit_topic_state(Partition& p, TopicReason reason, uint64_t now) noexcept;
    void emit_symbol_mapped(uint32_t id, uint64_t now) noexcept;
    void apply_transport_changes(Partition& p, Transport& t) noexcept;
    void service_timeouts(uint64_t now) noexcept;
    void apply_commands(uint64_t now) noexcept;
    void heartbeat_rings(uint64_t now) noexcept;
    void publish_partition_state(Partition& p) noexcept;

    // Recap (architecture.md, "Recap"). Pipeline thread only.
    const RecapPending* recap_hold(Partition& p, const TickHeader& h, uint64_t stream_seq, const uint8_t* bytes, uint32_t len) noexcept;
    void recap_symbol_stale(Partition& p, uint32_t symbol_id, SymbolStaleReason reason, uint64_t now) noexcept;
    void recap_track(Session& s, const FilterTable* before, const FilterTable* after, uint64_t now) noexcept;
    void recap_add_waiter(Partition& p, Session& s, uint32_t symbol_id, uint64_t now, bool bulk = false) noexcept;
    // The topic's other recap partitions in this pipeline (cold; at most 64).
    template <class F> void recap_siblings(const Partition& p, F f) noexcept;
    bool recap_homed_elsewhere(const Partition& p, uint32_t symbol_id) const noexcept;
    bool recap_waits_elsewhere(const Partition& p, uint32_t symbol_id, uint16_t slot) noexcept;
    void recap_drop_waiter(Partition& p, RecapPending& e, Session& s) noexcept;
    void recap_drop_session(Session& s) noexcept;
    void recap_restart(Partition& p, SymbolStaleReason reason, uint64_t now) noexcept;
    void recap_rebase(Partition& p, uint64_t seq) noexcept;
    void apply_recaps(uint64_t now) noexcept;
    void recap_reply(Partition& p, const RecapReply& r, uint64_t now) noexcept;
    void recap_consider(Partition& p, RecapPending& e, const RecapReply& r, uint64_t now) noexcept;
    void recap_merge(Partition& p, RecapPending& e, const RecapReply& r, uint64_t now) noexcept;
    void recap_emit(Partition& p, const RecapPending& e, ControlCode code, const RecapEvent& ev, uint64_t now) noexcept;
    void recap_post(Partition& p, const RecapCommand& c) noexcept;
    void recap_release(Partition& p, uint16_t slab) noexcept;
    void recap_flush() noexcept;

    std::string   name_;
    uint64_t      epoch_;
    TableResolver resolver_;
    Lvc&          lvc_;
    std::vector<Partition*> partitions_;
    uint64_t      partition_mask_ = 0;

    // All sessions attached to any partition of this group (for group-wide events).
    std::inplace_vector<Session*, Partition::kMaxSessions> sessions_;

    SpscQueue<PipelineCommand, 1024> commands_;
    RecapChannel          recap_;
    // INTENT: preallocated at add_partition (INV-002). Requests and releases
    // the manager's queues cannot take yet wait here in order; dropping a
    // CANCEL would leave the manager retrying for nobody, dropping a release
    // would leak a reply slab.
    std::vector<std::unique_ptr<RecapState>> recap_states_;
    // INTENT: one array per topic that maps instrument_sequence, sized by the
    // symbol table at add_partition (INV-002), shared by the topic's partitions:
    // the instrument's count is the topic's, whichever partition carries it.
    std::vector<std::vector<uint64_t>> inst_states_;
    std::vector<std::vector<uint8_t>>  inst_homes_;
    uint32_t              max_symbols_ = 0;
    std::vector<RecapCommand> recap_out_;
    std::vector<RecapRelease> release_out_;
    size_t                recap_out_cap_ = 0;
    size_t                release_out_cap_ = 0;
    RecapWaiters          recap_slots_;                       // slots held by recap sessions
    std::inplace_vector<Session*, RecapWaiters::kSlots> merge_waiters_;   // recap_merge scratch
    uint32_t              merge_counts_[RecapWaiters::kSlots] = {};
    Partition*            by_id_[64] = {};
    uint64_t              posted_ = 0;
    std::atomic<uint64_t> applied_{0};

    std::thread           thread_;
    std::atomic<bool>     running_{false};
    std::atomic<bool>     stop_requested_{false};
    std::atomic<uint64_t> loops_{0};
    uint64_t              last_heartbeat_ns_ = 0;
    PacketBatch           batch_;
    std::string           name_buf_; // reserved; for SYMBOL_MAPPED names

    LogHistogram hist_recv_to_publish_;
};

} // namespace subplane
