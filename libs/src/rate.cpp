#include "subplane/lib/rate.h"

#include <stdexcept>

namespace subplane::rate {

namespace {
constexpr uint64_t kNsPerUs = 1000;
constexpr uint64_t kNsPerMs = 1'000'000;
constexpr double kNsPerS = 1e9;
}

std::string Config::check() const {
    if (path.empty()) return "rate: the bucket path is empty";
    for (const lib::Bucket& b : path) {
        if (b.name.empty()) return "rate: a bucket has no name";
        if (b.rate == 0) return "rate: bucket " + b.name + " has rate 0";
        if (b.burst == 0) return "rate: bucket " + b.name + " has burst 0";
        // INTENT: a bucket never holds more than its burst, so a smaller grant
        // could never be met and the limiter would wait forever.
        if (min_grant > b.burst) return "rate: min_grant is above the burst of bucket " + b.name;
    }
    if (lease == 0) return "rate: lease is 0";
    if (min_grant == 0 || min_grant > lease) return "rate: min_grant must be 1..lease";
    if (low_water > lease) return "rate: low_water is above lease";
    if (retry_ms == 0) return "rate: retry_ms is 0";
    return {};
}

Limiter::Limiter(Config cfg) : cfg_(std::move(cfg)) {
    if (std::string err = cfg_.check(); !err.empty()) throw std::invalid_argument(err);
    if (cfg_.low_water == 0) cfg_.low_water = cfg_.lease / 2;
}

void Limiter::add_floor(uint64_t now_ns) {
    if (now_ns > floor_at_ns_ && cfg_.floor_rate != 0) {
        floor_acc_ += static_cast<double>(now_ns - floor_at_ns_) * static_cast<double>(cfg_.floor_rate) / kNsPerS;
        const int64_t have = tokens_.load(std::memory_order_relaxed);
        const auto room = static_cast<int64_t>(cfg_.lease) - have;
        const auto whole = static_cast<int64_t>(floor_acc_);
        const int64_t add = room <= 0 ? 0 : (whole < room ? whole : room);
        if (add > 0) {
            tokens_.fetch_add(add, std::memory_order_relaxed);
            stats_.floor_tokens.fetch_add(static_cast<uint64_t>(add), std::memory_order_relaxed);
        }
        // INTENT: a full lease drops the remainder instead of saving it, so a
        // long store outage cannot bank a burst for later.
        floor_acc_ = room <= whole ? 0 : floor_acc_ - static_cast<double>(add);
    }
    floor_at_ns_ = now_ns;
}

void Limiter::step(lib::Store& store, uint64_t now_ns) {
    const uint64_t have = available();
    const bool floor = stats_.floor.load(std::memory_order_relaxed);
    // INTENT: on the floor rate, try the store again at every retry even when
    // the floor has kept the lease up; otherwise a floor rate that keeps up with
    // demand would never find out the store is back.
    if ((have < cfg_.low_water || floor) && have < cfg_.lease && now_ns >= retry_at_ns_) refill(store, now_ns, have);
    if (stats_.floor.load(std::memory_order_relaxed)) add_floor(now_ns);
}

void Limiter::refill(lib::Store& store, uint64_t now_ns, uint64_t have) {
    const uint64_t want = cfg_.lease - have;
    const lib::Take t = store.take(cfg_.path, want, cfg_.min_grant < want ? cfg_.min_grant : want);
    if (t.status == lib::Take::ERROR) {
        stats_.store_errors.fetch_add(1, std::memory_order_relaxed);
        if (!stats_.floor.exchange(true, std::memory_order_relaxed)) {
            floor_at_ns_ = now_ns;
            floor_acc_ = 0;
        }
        retry_at_ns_ = now_ns + cfg_.retry_ms * kNsPerMs;
        return;
    }
    stats_.floor.store(false, std::memory_order_relaxed);
    if (t.granted != 0) {
        tokens_.fetch_add(static_cast<int64_t>(t.granted), std::memory_order_relaxed);
        stats_.granted.fetch_add(t.granted, std::memory_order_relaxed);
        retry_at_ns_ = 0;
    } else {
        stats_.denied.fetch_add(1, std::memory_order_relaxed);
        retry_at_ns_ = now_ns + t.wait_us * kNsPerUs;
    }
}

}  // namespace subplane::rate
