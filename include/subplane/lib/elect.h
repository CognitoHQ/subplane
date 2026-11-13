#pragma once
// subplane::elect: one primary among the redundant copies of a process.
//
// Election is per process: a whole process is primary or secondary, never a
// partition or topic inside it. A group names the copies (`gw.binance`: A on one
// host, B on another); each process runs one Candidate. The primary holds a
// lease in the store and renews it; it steps down on its own clock before the
// store can give the lease to another copy. Each new primary raises the group's
// epoch, a fencing token: what the primary produces carries it, and a consumer
// that drops a lower epoch is safe even if a store failover briefly allows two.

#include "subplane/lib/store.h"

#include <atomic>
#include <cstdint>
#include <string>

namespace subplane::elect {

struct Config {
    std::string group;        // the process's redundant copies share it
    std::string id;           // this copy; process_id() unless set
    uint32_t ttl_ms = 3000;   // lease length
    uint32_t margin_ms = 0;   // step down this long before the lease ends; 0: ttl / 10

    std::string check() const;
};

enum class Change : uint8_t {
    NONE,
    ELECTED,  // this copy is now primary, at epoch()
    LOST,     // the store says another copy holds the group
    EXPIRED,  // no renewal got through in time
    STOPPED,  // stop(): the lease was given up
};
const char* to_string(Change c) noexcept;

struct Stats {
    std::atomic<uint64_t> claims{0};        // claim attempts
    std::atomic<uint64_t> renews{0};        // successful renewals
    std::atomic<uint64_t> store_errors{0};
    std::atomic<uint64_t> transitions{0};   // ELECTED, LOST, EXPIRED, STOPPED
};

class Candidate {
public:
    explicit Candidate(Config cfg);  // throws std::invalid_argument when cfg.check() fails

    // "<host>:<pid>:<nonce>": a restarted process is a new candidate.
    static std::string process_id();

    // Any thread. Primary only while the lease is good by this process's clock,
    // so a stalled step() cannot leave a stale primary.
    bool is_primary(uint64_t now_ns) const noexcept {
        return now_ns < deadline_ns_.load(std::memory_order_acquire);
    }
    bool is_primary() const noexcept;
    uint64_t epoch() const noexcept { return epoch_.load(std::memory_order_acquire); }

    // Cold path, one thread: claims, renews, or steps down. `now_ns` is a
    // monotonic clock read before the call.
    Change step(lib::Store& store, uint64_t now_ns);
    // Gives the lease up (clean shutdown): STOPPED if this copy was primary.
    Change stop(lib::Store& store);

    const Config& config() const noexcept { return cfg_; }
    const Stats& stats() const noexcept { return stats_; }

private:
    Change demote(Change why);

    Config cfg_;
    Stats stats_;
    std::atomic<uint64_t> deadline_ns_{0};  // 0: secondary
    std::atomic<uint64_t> epoch_{0};
    bool primary_ = false;  // step() thread
    uint64_t next_ns_ = 0;  // next claim or renewal
};

}  // namespace subplane::elect
