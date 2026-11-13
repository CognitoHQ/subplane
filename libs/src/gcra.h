#pragma once
// GCRA over a path of buckets, in microseconds. FakeStore runs it in C++; the
// Valkey take script (valkey_store.cpp) is the same arithmetic in Lua: change
// both together.
//
// Per bucket: emission interval T = 1e6 / rate µs per token, burst tolerance
// tau = burst * T. The state is the theoretical arrival time `tat`; at `now` the
// bucket holds floor((now + tau - max(tat, now)) / T) tokens, and taking g tokens
// moves tat to max(tat, now) + g * T. Nothing drips: an idle bucket simply has
// an old tat, so state is one number per bucket.

#include <algorithm>
#include <cmath>
#include <cstdint>

namespace subplane::lib::gcra {

inline constexpr double kUsPerS = 1e6;

inline double interval_us(uint64_t rate) noexcept { return kUsPerS / static_cast<double>(rate); }

inline uint64_t available(double tat, double now, uint64_t rate, uint64_t burst) noexcept {
    const double t = interval_us(rate);
    const double tau = static_cast<double>(burst) * t;
    const double a = std::floor((now + tau - std::max(tat, now)) / t);
    return a > 0 ? static_cast<uint64_t>(a) : 0;
}

// Microseconds until the bucket holds `n` tokens (0 if it does now).
inline uint64_t wait_us(double tat, double now, uint64_t rate, uint64_t burst, uint64_t n) noexcept {
    const double t = interval_us(rate);
    const double w = std::max(tat, now) + static_cast<double>(n) * t - static_cast<double>(burst) * t - now;
    return w > 0 ? static_cast<uint64_t>(std::ceil(w)) : 0;
}

inline double after_take(double tat, double now, uint64_t rate, uint64_t granted) noexcept {
    return std::max(tat, now) + static_cast<double>(granted) * interval_us(rate);
}

}  // namespace subplane::lib::gcra
