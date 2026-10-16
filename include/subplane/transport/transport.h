#pragma once

// Transport component interface: delivers raw packets from a bus partition.
//
// Hot-path contract: exactly ONE virtual call per packet *batch* (poll). The
// batch's PacketViews point into transport-owned memory that stays valid until
// the next poll() on the same transport. No allocation after open().
// Up/down changes are reported from inside poll() through the non-virtual
// report_up/report_down, so state costs no extra virtual call.

#include <atomic>
#include <cstdint>
#include <memory>
#include <functional>
#include <string>
#include <string_view>

#include <yaml-cpp/yaml.h>

namespace subplane {

struct PacketView {
    const uint8_t* data;
    uint32_t       len;
    uint64_t       recv_ts_ns; // CLOCK_REALTIME ns at receive (kernel timestamp if enabled)
};

struct PacketBatch {
    static constexpr int kMax = 64;
    PacketView pkts[kMax];
    int        count = 0;
};

// Why a transport went down or came back. Transport-neutral (INV-015): the
// transport-specific cause stays in its own counters and last_error.
enum class TransportReason : uint8_t {
    NONE              = 0,
    SOURCE_LOST       = 1, // the remote end went away: Aeron image gone, TCP peer closed or reset
    LOCAL_FAILURE     = 2, // the agent's side failed: socket error, driver timeout, misframed stream
    PUBLISHER_RESTART = 3, // a different publisher instance is now the source (on TRANSPORT_UP)
};

enum class TransportState : uint8_t { UNKNOWN = 0, UP = 1, DOWN = 2 };

struct TransportStateChange {
    TransportState  state;  // UP or DOWN
    TransportReason reason;
};

class Transport {
public:
    static constexpr int kMaxStateChanges = 4;

    virtual ~Transport() = default;

    // Cold. Returns false with err set on failure.
    virtual bool open(std::string& err) = 0;

    // Hot. Fills out with up to batch-size packets without blocking.
    // Returns number of packets (0 if none), or -1 on a fatal socket error.
    virtual int poll(PacketBatch& out) noexcept = 0;

    // Cold.
    virtual void close() noexcept = 0;

    // Cold. Human-readable endpoint for logs/status.
    virtual std::string describe() const = 0;

    // Cold. File descriptor for epoll integration (-1 if none).
    virtual int fd() const noexcept = 0;

    // Cold. The transport's own counters for /metrics, by name ("fragments",
    // "connects"); each is a one-writer relaxed atomic the poll thread writes.
    virtual void counters(const std::function<void(std::string_view name, uint64_t value)>& out) const { (void)out; }

    // Hot, non-virtual. State changes the transport noticed during poll(), in
    // order. The pipeline reads them right after poll() and before the batch's
    // packets, then calls clear_state_changes().
    int state_change_count() const noexcept { return n_changes_; }
    const TransportStateChange& state_change(int i) const noexcept { return changes_[i]; }
    void clear_state_changes() noexcept { n_changes_ = 0; }

    // Cold. Current state, for /partitions (relaxed: written by the poll thread).
    TransportState state() const noexcept { return state_pub_.load(std::memory_order_relaxed); }

protected:
    // Hot, called from poll() on the pipeline thread only. Repeats are dropped:
    // UP while up and DOWN while down report nothing, except UP with
    // PUBLISHER_RESTART, which reports a new source even without a gap.
    void report_up(TransportReason r = TransportReason::NONE) noexcept {
        if (state_ == TransportState::UP && r != TransportReason::PUBLISHER_RESTART) return;
        set_state(TransportState::UP, r);
    }
    void report_down(TransportReason r) noexcept {
        if (state_ == TransportState::DOWN) return;
        set_state(TransportState::DOWN, r);
    }

private:
    void set_state(TransportState s, TransportReason r) noexcept {
        state_ = s;
        state_pub_.store(s, std::memory_order_relaxed);
        // INTENT: a poll reports at most a DOWN and an UP or two; if the
        // pipeline ever fails to drain, keep the latest change in the last
        // slot so the final state is never lost.
        const int i = n_changes_ < kMaxStateChanges ? n_changes_++ : kMaxStateChanges - 1;
        changes_[i] = TransportStateChange{s, r};
    }

    TransportState              state_ = TransportState::UNKNOWN;
    std::atomic<TransportState> state_pub_{TransportState::UNKNOWN};
    int                         n_changes_ = 0;
    TransportStateChange        changes_[kMaxStateChanges] = {};
};

inline const char* transport_state_name(TransportState s) noexcept {
    switch (s) {
        case TransportState::UP:   return "up";
        case TransportState::DOWN: return "down";
        default:                   return "unknown";
    }
}

} // namespace subplane
