#include "subplane/lib/elect.h"

#include "subplane/lib/runner.h"

#include <cstdio>
#include <limits.h>
#include <random>
#include <stdexcept>
#include <unistd.h>

namespace subplane::elect {

namespace {
constexpr uint64_t kNsPerMs = 1'000'000;
constexpr uint32_t kRenewsPerTtl = 3;     // renew (or, as secondary, try to claim) every ttl / 3
constexpr uint32_t kMarginDivisor = 10;   // default margin: ttl / 10
}

const char* to_string(Change c) noexcept {
    switch (c) {
        case Change::NONE: return "none";
        case Change::ELECTED: return "elected";
        case Change::LOST: return "lost";
        case Change::EXPIRED: return "expired";
        case Change::STOPPED: return "stopped";
    }
    return "?";
}

std::string Config::check() const {
    if (group.empty()) return "elect: group is empty";
    if (ttl_ms < kRenewsPerTtl) return "elect: ttl_ms is too small";
    if (margin_ms >= ttl_ms / kRenewsPerTtl) return "elect: margin_ms must be below ttl_ms / 3";
    return {};
}

std::string Candidate::process_id() {
    char host[HOST_NAME_MAX + 1] = {};
    if (gethostname(host, sizeof(host) - 1) != 0) host[0] = '\0';
    std::random_device rd;
    const uint64_t nonce = (static_cast<uint64_t>(rd()) << 32) | rd();
    char buf[sizeof(host) + 64];
    std::snprintf(buf, sizeof(buf), "%s:%d:%016llx", host, static_cast<int>(getpid()),
                  static_cast<unsigned long long>(nonce));
    return buf;
}

Candidate::Candidate(Config cfg) : cfg_(std::move(cfg)) {
    if (std::string err = cfg_.check(); !err.empty()) throw std::invalid_argument(err);
    if (cfg_.id.empty()) cfg_.id = process_id();
    if (cfg_.margin_ms == 0) cfg_.margin_ms = cfg_.ttl_ms / kMarginDivisor;
}

bool Candidate::is_primary() const noexcept { return is_primary(lib::now_ns()); }

Change Candidate::demote(Change why) {
    primary_ = false;
    deadline_ns_.store(0, std::memory_order_release);
    stats_.transitions.fetch_add(1, std::memory_order_relaxed);
    return why;
}

Change Candidate::step(lib::Store& store, uint64_t now_ns) {
    const uint64_t ttl_ns = cfg_.ttl_ms * kNsPerMs;
    const uint64_t every_ns = ttl_ns / kRenewsPerTtl;
    // INTENT: the lease is measured from `now_ns`, read before the call went
    // out, so it ends here no later than in the store (which starts its TTL when
    // the call arrives). Minus the margin for clock-rate drift: the old primary
    // stops before the store can hand the group to another copy.
    const uint64_t lease_end_ns = now_ns + ttl_ns - cfg_.margin_ms * kNsPerMs;

    if (primary_) {
        if (now_ns >= deadline_ns_.load(std::memory_order_relaxed)) return demote(Change::EXPIRED);
        if (now_ns < next_ns_) return Change::NONE;
        switch (store.renew(cfg_.group, cfg_.id, epoch_.load(std::memory_order_relaxed), cfg_.ttl_ms)) {
            case lib::Renew::OK:
                stats_.renews.fetch_add(1, std::memory_order_relaxed);
                deadline_ns_.store(lease_end_ns, std::memory_order_release);
                next_ns_ = now_ns + every_ns;
                return Change::NONE;
            case lib::Renew::LOST:
                next_ns_ = now_ns + every_ns;
                return demote(Change::LOST);
            case lib::Renew::ERROR:
                // Keep trying until the deadline; each try is a fresh chance.
                stats_.store_errors.fetch_add(1, std::memory_order_relaxed);
                next_ns_ = now_ns + cfg_.margin_ms * kNsPerMs;
                return Change::NONE;
        }
        return Change::NONE;
    }

    if (now_ns < next_ns_) return Change::NONE;
    next_ns_ = now_ns + every_ns;
    stats_.claims.fetch_add(1, std::memory_order_relaxed);
    const lib::Claim c = store.claim(cfg_.group, cfg_.id, cfg_.ttl_ms);
    if (c.status == lib::Claim::ERROR) {
        stats_.store_errors.fetch_add(1, std::memory_order_relaxed);
        return Change::NONE;
    }
    if (c.status == lib::Claim::TAKEN) return Change::NONE;
    primary_ = true;
    epoch_.store(c.epoch, std::memory_order_release);
    deadline_ns_.store(lease_end_ns, std::memory_order_release);
    stats_.transitions.fetch_add(1, std::memory_order_relaxed);
    return Change::ELECTED;
}

Change Candidate::stop(lib::Store& store) {
    if (!primary_) return Change::NONE;
    store.release(cfg_.group, cfg_.id, epoch_.load(std::memory_order_relaxed));
    return demote(Change::STOPPED);
}

}  // namespace subplane::elect
