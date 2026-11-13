#include "subplane/lib/runner.h"

#include "subplane/lib/elect.h"
#include "subplane/lib/rate.h"

#include <chrono>

namespace subplane::lib {

uint64_t now_ns() noexcept {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                     std::chrono::steady_clock::now().time_since_epoch())
                                     .count());
}

Runner::Runner(Store& store, uint32_t tick_ms) : store_(store), tick_ms_(tick_ms) {}

Runner::~Runner() { stop(); }

void Runner::start() {
    if (running_.exchange(true)) return;
    thread_ = std::thread([this] { run(); });
}

void Runner::stop() {
    if (!running_.exchange(false)) return;
    if (thread_.joinable()) thread_.join();
    for (elect::Candidate* c : candidates_) c->stop(store_);
}

void Runner::run() {
    while (running_.load(std::memory_order_relaxed)) {
        for (elect::Candidate* c : candidates_) c->step(store_, now_ns());
        for (rate::Limiter* l : limiters_) l->step(store_, now_ns());
        std::this_thread::sleep_for(std::chrono::milliseconds(tick_ms_));
    }
}

}  // namespace subplane::lib
