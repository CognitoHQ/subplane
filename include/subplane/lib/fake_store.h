#pragma once
// subplane-lib: an in-process Store on a clock the caller sets. The same rules
// as ValkeyStore's scripts, so the logic is tested without a server; hosts can
// use it in their own tests.

#include "subplane/lib/store.h"

#include <cstdint>
#include <map>
#include <string>

namespace subplane::lib {

class FakeStore final : public Store {
public:
    void set_now_us(uint64_t us) noexcept { now_us_ = us; }
    void advance_us(uint64_t us) noexcept { now_us_ += us; }
    // Unreachable: every call fails, as with a dead server.
    void set_down(bool down) noexcept { down_ = down; }
    // Ends a group's lease now, as when it lapses in the store.
    void expire(std::string_view group);

    Take take(std::span<const Bucket> path, uint64_t want, uint64_t min) override;
    Claim claim(std::string_view group, std::string_view me, uint32_t ttl_ms) override;
    Renew renew(std::string_view group, std::string_view me, uint64_t epoch, uint32_t ttl_ms) override;
    void release(std::string_view group, std::string_view me, uint64_t epoch) override;

private:
    struct Lease {
        std::string holder;
        uint64_t epoch = 0;
        uint64_t until_us = 0;
    };
    Lease* live(std::string_view group);

    uint64_t now_us_ = 0;
    bool down_ = false;
    std::map<std::string, double, std::less<>> tat_;  // GCRA theoretical arrival time, µs
    std::map<std::string, Lease, std::less<>> leases_;
    std::map<std::string, uint64_t, std::less<>> epochs_;
};

}  // namespace subplane::lib
