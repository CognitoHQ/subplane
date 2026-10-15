#pragma once

// Fixed-capacity single-producer single-consumer queue. Used for the pipeline
// command queue and the recap queues between pipelines and the recap manager.

#include <array>
#include <atomic>
#include <cstddef>

namespace subplane {

// INTENT: exactly one producer thread and one consumer thread at a time (the
// pipeline command queue gets that from the SessionManager mutex). Fixed
// capacity, no allocation. A second producer would need a mutex on its side.
template <class T, size_t N>
class SpscQueue {
    static_assert((N & (N - 1)) == 0, "N must be a power of two");
public:
    bool push(const T& v) noexcept {
        const size_t t = tail_.load(std::memory_order_relaxed);
        if (t - head_.load(std::memory_order_acquire) >= N) return false;
        buf_[t & (N - 1)] = v;
        tail_.store(t + 1, std::memory_order_release);
        return true;
    }
    bool pop(T& out) noexcept {
        const size_t h = head_.load(std::memory_order_relaxed);
        if (h == tail_.load(std::memory_order_acquire)) return false;
        out = buf_[h & (N - 1)];
        head_.store(h + 1, std::memory_order_release);
        return true;
    }
private:
    alignas(64) std::atomic<size_t> head_{0};
    alignas(64) std::atomic<size_t> tail_{0};
    std::array<T, N> buf_{};
};

} // namespace subplane
