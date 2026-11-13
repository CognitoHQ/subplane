#pragma once
// subplane-lib: the shared state behind rate limits and elections.
//
// A Store holds what several processes must agree on: GCRA bucket state for
// rate limits, and the holder and epoch of each election group. ValkeyStore
// keeps it in Valkey (one Lua script per call, so each decision is atomic);
// FakeStore keeps it in process for tests. Every call is cold path: the
// library calls a store from a Runner thread or the caller's cold loop, never
// from a spend (rate::Limiter::try_acquire) or a role check.
//
// C++20, like the app SDK, so other repos (subplane-feeds) can link it.

#include <cstdint>
#include <span>
#include <string>
#include <string_view>

namespace subplane::lib {

// One node of a rate tree (site > interface > topic > partition). Tokens are
// whatever the caller counts: bytes, messages, requests. Every client naming a
// bucket must use the same rate and burst; the store keeps only its state.
struct Bucket {
    std::string name;    // unique within the store, e.g. "site/eth0/l1.crypto"
    uint64_t rate = 0;   // tokens per second
    uint64_t burst = 0;  // tokens available after an idle period
};

struct Take {
    enum Status : uint8_t { OK, ERROR };
    Status status = ERROR;
    uint64_t granted = 0;  // 0 when any bucket on the path has fewer than `min`
    uint64_t wait_us = 0;  // when granted is 0: until every bucket has `min`
};

struct Claim {
    enum Status : uint8_t { HELD, TAKEN, ERROR };
    Status status = ERROR;  // HELD: this candidate is the primary
    uint64_t epoch = 0;     // the group's current epoch (HELD or TAKEN)
};

enum class Renew : uint8_t { OK, LOST, ERROR };

class Store {
public:
    virtual ~Store() = default;

    // Takes up to `want` tokens from every bucket on `path` at once: the grant
    // is what the emptiest bucket allows, or 0 when that is below `min`.
    virtual Take take(std::span<const Bucket> path, uint64_t want, uint64_t min) = 0;

    // Election group `group`: become the holder if there is none (raising the
    // group's epoch), or keep holding if `me` already is. Either way the lease
    // runs `ttl_ms` from when the store applies it.
    virtual Claim claim(std::string_view group, std::string_view me, uint32_t ttl_ms) = 0;
    // Extends the lease if `me` still holds `group` at `epoch`; LOST otherwise.
    virtual Renew renew(std::string_view group, std::string_view me, uint64_t epoch, uint32_t ttl_ms) = 0;
    // Gives the lease up if `me` holds it at `epoch`; a no-op otherwise.
    virtual void release(std::string_view group, std::string_view me, uint64_t epoch) = 0;
};

}  // namespace subplane::lib
