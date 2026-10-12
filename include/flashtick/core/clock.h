#pragma once

#include <cstdint>
#include <ctime>

namespace flashtick {

// INTENT: vDSO-backed clock_gettime; no syscall on Linux. REALTIME is used for
// timestamps that cross process boundaries (ingest_ts, INV-006); MONOTONIC for
// durations, since REALTIME can step under NTP.
inline uint64_t now_ns() noexcept {
    timespec ts;
    ::clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ull + static_cast<uint64_t>(ts.tv_nsec);
}

inline uint64_t now_mono_ns() noexcept {
    timespec ts;
    ::clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ull + static_cast<uint64_t>(ts.tv_nsec);
}

} // namespace flashtick
