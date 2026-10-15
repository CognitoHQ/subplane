#pragma once

// One pipeline thread group: owns a set of partitions and drives
// poll -> frame -> decode -> filter -> publish for all sessions attached to
// those partitions. Everything in the loop is single-threaded; the cold side
// talks to it only through the SPSC command queue.

#include "subplane/core/histogram.h"
#include "subplane/core/spsc_queue.h"
#include "subplane/pipeline/lvc.h"
#include "subplane/pipeline/partition.h"
#include "subplane/pipeline/session.h"
#include "subplane/pipeline/symbol_resolver.h"
#include "subplane/recap/recap_manager.h"

#include <array>
#include <atomic>
#include <cstdint>
#include <string>
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
    // Recap queues to and from the agent's RecapManager (consumed from step 3.4).
    RecapChannel& recap_channel() noexcept { return recap_; }

private:
    void loop() noexcept;
    void process_packet(Partition& p, const PacketView& pkt) noexcept;
    void process_message(Partition& p, const uint8_t* msg, uint32_t len, uint64_t recv_ts,
                         const FrameInfo& info) noexcept;
    // Returns false if the packet/message is a duplicate and must be dropped.
    bool check_sequence(Partition& p, uint64_t seq, uint64_t now) noexcept;
    void publish_data(Partition& p, const TickHeader& h, const uint8_t* bytes, uint32_t len) noexcept;
    void publish_admin(Partition& p, const TickHeader& h, const uint8_t* bytes, uint32_t len) noexcept;
    void emit_control(Partition& p, ControlCode code, const void* body, uint32_t len, uint64_t now) noexcept;
    void emit_control_all(ControlCode code, const void* body, uint32_t len, uint64_t now) noexcept;
    void emit_symbol_mapped(uint32_t id, uint64_t now) noexcept;
    void apply_transport_changes(Partition& p, Transport& t) noexcept;
    void service_timeouts(uint64_t now) noexcept;
    void apply_commands(uint64_t now) noexcept;
    void heartbeat_rings(uint64_t now) noexcept;
    void publish_partition_state(Partition& p) noexcept;

    std::string   name_;
    uint64_t      epoch_;
    TableResolver resolver_;
    Lvc&          lvc_;
    std::vector<Partition*> partitions_;
    uint64_t      partition_mask_ = 0;

    // All sessions attached to any partition of this group (for group-wide events).
    Session*  sessions_[Partition::kMaxSessions] = {};
    uint32_t  n_sessions_ = 0;

    SpscQueue<PipelineCommand, 1024> commands_;
    RecapChannel          recap_;
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
