#include "subplane/lib/fake_store.h"

#include "gcra.h"

#include <algorithm>
#include <vector>

namespace subplane::lib {

namespace {
constexpr uint64_t kUsPerMs = 1000;
}

Take FakeStore::take(std::span<const Bucket> path, uint64_t want, uint64_t min) {
    if (down_) return {};
    const auto now = static_cast<double>(now_us_);
    std::vector<double> tat(path.size(), 0.0);
    uint64_t grant = want;
    for (size_t i = 0; i < path.size(); ++i) {
        auto it = tat_.find(path[i].name);
        tat[i] = it == tat_.end() ? now : it->second;
        grant = std::min(grant, gcra::available(tat[i], now, path[i].rate, path[i].burst));
    }
    if (grant < min || grant == 0) {
        uint64_t wait = 0;
        for (size_t i = 0; i < path.size(); ++i)
            wait = std::max(wait, gcra::wait_us(tat[i], now, path[i].rate, path[i].burst, std::max<uint64_t>(min, 1)));
        return {Take::OK, 0, wait};
    }
    for (size_t i = 0; i < path.size(); ++i)
        tat_[path[i].name] = gcra::after_take(tat[i], now, path[i].rate, grant);
    return {Take::OK, grant, 0};
}

FakeStore::Lease* FakeStore::live(std::string_view group) {
    auto it = leases_.find(group);
    if (it == leases_.end()) return nullptr;
    if (it->second.until_us <= now_us_) {
        leases_.erase(it);
        return nullptr;
    }
    return &it->second;
}

void FakeStore::expire(std::string_view group) {
    if (auto it = leases_.find(group); it != leases_.end()) leases_.erase(it);
}

Claim FakeStore::claim(std::string_view group, std::string_view me, uint32_t ttl_ms) {
    if (down_) return {};
    Lease* l = live(group);
    if (l == nullptr) {
        auto e = epochs_.find(group);
        if (e == epochs_.end()) e = epochs_.emplace(std::string(group), 0).first;
        const uint64_t epoch = ++e->second;
        leases_[std::string(group)] = Lease{std::string(me), epoch, now_us_ + ttl_ms * kUsPerMs};
        return {Claim::HELD, epoch};
    }
    if (l->holder == me) {
        l->until_us = now_us_ + ttl_ms * kUsPerMs;
        return {Claim::HELD, l->epoch};
    }
    return {Claim::TAKEN, l->epoch};
}

Renew FakeStore::renew(std::string_view group, std::string_view me, uint64_t epoch, uint32_t ttl_ms) {
    if (down_) return Renew::ERROR;
    Lease* l = live(group);
    if (l == nullptr || l->holder != me || l->epoch != epoch) return Renew::LOST;
    l->until_us = now_us_ + ttl_ms * kUsPerMs;
    return Renew::OK;
}

void FakeStore::release(std::string_view group, std::string_view me, uint64_t epoch) {
    if (down_) return;
    Lease* l = live(group);
    if (l != nullptr && l->holder == me && l->epoch == epoch) leases_.erase(leases_.find(group));
}

}  // namespace subplane::lib
