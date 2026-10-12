#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>

namespace flashtick {

// Log-linear nanosecond histogram: 64 power-of-two majors x 4 sub-buckets = 256.
// Value 0 and 1 share bucket 0. Relative error per bucket <= 25%.
//
// INTENT: single writer (the owning pipeline thread). record() therefore uses
// relaxed load+store instead of lock-prefixed RMWs: no bus-locked instruction on
// the hot path. If a second thread ever records into the same instance, counts
// will be lost silently; give each writer its own histogram. Readers (metrics,
// cold) see eventually-consistent values; count/sum/buckets may be momentarily
// skewed against each other, which is acceptable for monitoring.
struct alignas(64) LogHistogram {
    static constexpr unsigned kBuckets = 256;

    struct Snapshot {
        uint64_t count = 0, sum = 0, min = 0, max = 0;
        uint64_t buckets[kBuckets] = {};
    };

    std::atomic<uint64_t> buckets[kBuckets] = {};
    std::atomic<uint64_t> count{0};
    std::atomic<uint64_t> sum{0};
    std::atomic<uint64_t> min{UINT64_MAX};
    std::atomic<uint64_t> max{0};

    static constexpr unsigned bucket_index(uint64_t v) noexcept {
        if (v == 0) return 0;
        const unsigned major = 63u - static_cast<unsigned>(__builtin_clzll(v));
        unsigned minor = 0;
        if (major >= 2) minor = static_cast<unsigned>(v >> (major - 2)) & 3u;
        else if (major == 1) minor = (static_cast<unsigned>(v) & 1u) << 1;
        return major * 4u + minor;
    }

    // Inclusive lower edge of the bucket.
    static constexpr uint64_t bucket_lower_edge(unsigned idx) noexcept {
        const unsigned major = idx >> 2, minor = idx & 3u;
        if (idx == 0) return 0;
        if (major == 0) return 1;
        if (major == 1) return minor < 2 ? 2 : 3;
        return static_cast<uint64_t>(4u + minor) << (major - 2);
    }

    // Inclusive upper edge of the bucket.
    static constexpr uint64_t bucket_upper_edge(unsigned idx) noexcept {
        const unsigned major = idx >> 2, minor = idx & 3u;
        if (idx == 0) return 1;
        if (major == 0) return 1;
        if (major == 1) return minor < 2 ? 2 : 3;
        if (idx == kBuckets - 1) return UINT64_MAX;
        return bucket_lower_edge(idx + 1) - 1;
    }

    // Hot, single writer. min/max use plain relaxed compare+store (not CAS):
    // with one writer nobody can race the read-modify-write.
    void record(uint64_t ns) noexcept {
        auto& b = buckets[bucket_index(ns)];
        b.store(b.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
        count.store(count.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
        sum.store(sum.load(std::memory_order_relaxed) + ns, std::memory_order_relaxed);
        if (ns < min.load(std::memory_order_relaxed)) min.store(ns, std::memory_order_relaxed);
        if (ns > max.load(std::memory_order_relaxed)) max.store(ns, std::memory_order_relaxed);
    }

    // Cold.
    Snapshot snapshot() const {
        Snapshot s;
        for (unsigned i = 0; i < kBuckets; ++i) s.buckets[i] = buckets[i].load(std::memory_order_relaxed);
        s.count = count.load(std::memory_order_relaxed);
        s.sum   = sum.load(std::memory_order_relaxed);
        s.min   = min.load(std::memory_order_relaxed);
        s.max   = max.load(std::memory_order_relaxed);
        if (s.count == 0) s.min = 0;
        return s;
    }
};

// Upper edge of the bucket holding the p-th sample (p in [0,1]). 0 if empty.
inline uint64_t percentile(const LogHistogram::Snapshot& s, double p) {
    if (s.count == 0) return 0;
    if (p < 0.0) p = 0.0;
    if (p > 1.0) p = 1.0;
    double rankf = p * static_cast<double>(s.count);
    uint64_t rank = static_cast<uint64_t>(rankf);
    if (static_cast<double>(rank) < rankf) ++rank;
    if (rank == 0) rank = 1;
    uint64_t cum = 0;
    for (unsigned i = 0; i < LogHistogram::kBuckets; ++i) {
        cum += s.buckets[i];
        if (cum >= rank) return LogHistogram::bucket_upper_edge(i);
    }
    return LogHistogram::bucket_upper_edge(LogHistogram::kBuckets - 1);
}

inline double mean(const LogHistogram::Snapshot& s) {
    return s.count == 0 ? 0.0 : static_cast<double>(s.sum) / static_cast<double>(s.count);
}

// Cold. `name` is emitted verbatim inside quotes: callers pass identifier-like names.
inline std::string to_json(const LogHistogram::Snapshot& s, std::string_view name) {
    char buf[384];
    std::snprintf(buf, sizeof buf,
                  "\"count\":%llu,\"sum\":%llu,\"min\":%llu,\"max\":%llu,\"mean\":%.1f,"
                  "\"p50\":%llu,\"p90\":%llu,\"p99\":%llu,\"p999\":%llu}",
                  static_cast<unsigned long long>(s.count), static_cast<unsigned long long>(s.sum),
                  static_cast<unsigned long long>(s.min), static_cast<unsigned long long>(s.max), mean(s),
                  static_cast<unsigned long long>(percentile(s, 0.50)),
                  static_cast<unsigned long long>(percentile(s, 0.90)),
                  static_cast<unsigned long long>(percentile(s, 0.99)),
                  static_cast<unsigned long long>(percentile(s, 0.999)));
    std::string out = "{\"name\":\"";
    out.append(name);
    out += "\",";
    out += buf;
    return out;
}

} // namespace flashtick
