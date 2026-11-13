#pragma once
// subplane-lib: a thread that drives Limiters and Candidates against a store,
// so the host's own threads never wait on the network.

#include "subplane/lib/store.h"

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

namespace subplane::rate { class Limiter; }
namespace subplane::elect { class Candidate; }

namespace subplane::lib {

uint64_t now_ns() noexcept;  // the monotonic clock every step() takes

class Runner {
public:
    Runner(Store& store, uint32_t tick_ms);
    ~Runner();
    Runner(const Runner&) = delete;
    Runner& operator=(const Runner&) = delete;

    // Before start().
    void add(rate::Limiter& l) { limiters_.push_back(&l); }
    void add(elect::Candidate& c) { candidates_.push_back(&c); }

    void start();
    // Joins the thread, then gives up every lease a candidate holds.
    void stop();

private:
    void run();

    Store& store_;
    uint32_t tick_ms_;
    std::vector<rate::Limiter*> limiters_;
    std::vector<elect::Candidate*> candidates_;
    std::atomic<bool> running_{false};
    std::thread thread_;
};

}  // namespace subplane::lib
