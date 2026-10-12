#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace flashtick {

// Single-writer, many-reader seqlock for trivially copyable T.
//
// INTENT: the writer (hot path) never waits and never allocates; readers (cold:
// control channel, HTTP) retry on a torn read. The payload is held as relaxed
// atomic 64-bit words rather than raw bytes so concurrent reader/writer access
// is not a C++ data race; relaxed word stores compile to plain moves. The
// version is odd while a write is in progress. Removing the release fence lets
// the payload stores be reordered before the odd version becomes visible, so a
// reader could validate a torn value. Two concurrent writers are NOT supported.
template <class T>
class Seqlock {
    static_assert(std::is_trivially_copyable_v<T>, "Seqlock<T> requires trivially copyable T");
    static constexpr std::size_t kWords = (sizeof(T) + 7) / 8;

public:
    Seqlock() noexcept {
        for (auto& w : words_) w.store(0, std::memory_order_relaxed);
    }

    // Hot, writer thread only.
    void store(const T& v) noexcept {
        uint64_t tmp[kWords] = {};
        std::memcpy(tmp, &v, sizeof(T));
        const uint64_t ver = ver_.load(std::memory_order_relaxed);
        ver_.store(ver + 1, std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_release);
        for (std::size_t i = 0; i < kWords; ++i) words_[i].store(tmp[i], std::memory_order_relaxed);
        ver_.store(ver + 2, std::memory_order_release);
    }

    // Cold reader. Returns false if no consistent copy was obtained within the
    // bounded number of spins (writer busy / continuously writing).
    bool load(T& out) const noexcept {
        constexpr int kMaxSpins = 1000;
        for (int spin = 0; spin < kMaxSpins; ++spin) {
            const uint64_t v1 = ver_.load(std::memory_order_acquire);
            if (v1 & 1u) continue;
            uint64_t tmp[kWords];
            for (std::size_t i = 0; i < kWords; ++i) tmp[i] = words_[i].load(std::memory_order_relaxed);
            std::atomic_thread_fence(std::memory_order_acquire);
            if (ver_.load(std::memory_order_relaxed) == v1) {
                std::memcpy(&out, tmp, sizeof(T));
                return true;
            }
        }
        return false;
    }

    // Number of completed writes * 2 (even when stable).
    uint64_t version() const noexcept { return ver_.load(std::memory_order_acquire); }

private:
    alignas(64) std::atomic<uint64_t> ver_{0};
    std::atomic<uint64_t> words_[kWords];
};

} // namespace flashtick
