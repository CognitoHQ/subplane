#pragma once
// subplane::rate: hierarchical rate limits shared across processes.
//
// A Limiter spends from a local lease of tokens (one atomic per spend, no I/O)
// and refills it from the store, which charges every bucket on its path (site >
// interface > topic > ...) in one atomic call. When the store is unreachable the
// Limiter refills at its own floor rate: never unlimited, never stopped.

#include "subplane/lib/store.h"

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

namespace subplane::rate {

struct Config {
    std::vector<lib::Bucket> path;  // root first; the last is this caller's own bucket
    uint64_t lease = 0;             // tokens asked per refill; the largest single spend
    uint64_t low_water = 0;         // refill when the lease is below this; 0: lease / 2
    uint64_t min_grant = 1;         // a refill smaller than this is refused and retried
    uint64_t floor_rate = 0;        // tokens per second while the store is unreachable
    uint32_t retry_ms = 100;        // after a store error

    // Empty when usable, else what is wrong.
    std::string check() const;
};

// Relaxed counters, read by the host for its metrics.
struct Stats {
    std::atomic<uint64_t> granted{0};       // tokens leased from the store
    std::atomic<uint64_t> denied{0};        // refills refused (a bucket on the path was empty)
    std::atomic<uint64_t> throttled{0};     // try_acquire refusals
    std::atomic<uint64_t> store_errors{0};
    std::atomic<uint64_t> floor_tokens{0};  // tokens added at the floor rate
    std::atomic<bool> floor{false};         // the store is unreachable: refilling at the floor rate
};

class Limiter {
public:
    explicit Limiter(Config cfg);  // throws std::invalid_argument when cfg.check() fails

    // Hot path, any thread: takes n tokens from the lease, or none.
    bool try_acquire(uint64_t n) noexcept {
        const auto want = static_cast<int64_t>(n);
        int64_t cur = tokens_.load(std::memory_order_relaxed);
        do {
            if (cur < want) {
                stats_.throttled.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
        } while (!tokens_.compare_exchange_weak(cur, cur - want, std::memory_order_relaxed));
        return true;
    }
    // Returns tokens taken but not used (an offer that did not go out).
    void give_back(uint64_t n) noexcept { tokens_.fetch_add(static_cast<int64_t>(n), std::memory_order_relaxed); }
    uint64_t available() const noexcept {
        const int64_t t = tokens_.load(std::memory_order_relaxed);
        return t > 0 ? static_cast<uint64_t>(t) : 0;
    }

    // Cold path, one thread (a Runner or the caller's cold loop): refills the
    // lease when it is low. `now_ns` is a monotonic clock.
    void step(lib::Store& store, uint64_t now_ns);

    const Config& config() const noexcept { return cfg_; }
    const Stats& stats() const noexcept { return stats_; }

private:
    void refill(lib::Store& store, uint64_t now_ns, uint64_t have);
    void add_floor(uint64_t now_ns);

    Config cfg_;
    Stats stats_;
    std::atomic<int64_t> tokens_{0};
    // step() state, one thread.
    uint64_t retry_at_ns_ = 0;
    uint64_t floor_at_ns_ = 0;
    double floor_acc_ = 0;
};

}  // namespace subplane::rate
