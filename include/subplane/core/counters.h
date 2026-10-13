#pragma once

#include <atomic>
#include <cstdint>
#include <string>

namespace subplane {

// INTENT: every counter has exactly one writer thread (partition thread for
// PartitionCounters, the session's publisher for SessionCounters). A relaxed
// load+store avoids a lock-prefixed fetch_add on the hot path and is still
// exact with one writer. With two writers increments would be lost: do not share.
inline void inc(std::atomic<uint64_t>& c, uint64_t n = 1) noexcept {
    c.store(c.load(std::memory_order_relaxed) + n, std::memory_order_relaxed);
}

#define SUBPLANE_PARTITION_COUNTERS(X) \
    X(packets) X(bytes) X(messages) X(unknown_templates) X(undecodable_tail) X(schema_mismatch) \
    X(gaps) X(dups) X(admin) X(degraded_events) X(decode_errors) X(malformed) X(truncated) X(transport_errors) X(seq_resets)

#define SUBPLANE_SESSION_COUNTERS(X) X(published) X(filtered) X(ring_overruns) X(publish_failed)

struct alignas(64) PartitionCounters {
    struct Snapshot {
#define X(n) uint64_t n = 0;
        SUBPLANE_PARTITION_COUNTERS(X)
#undef X
        uint32_t last_unknown_template = 0;
    };

#define X(n) std::atomic<uint64_t> n{0};
    SUBPLANE_PARTITION_COUNTERS(X)
#undef X
    std::atomic<uint32_t> last_unknown_template{0};

    Snapshot snapshot() const {
        Snapshot s;
#define X(n) s.n = n.load(std::memory_order_relaxed);
        SUBPLANE_PARTITION_COUNTERS(X)
#undef X
        s.last_unknown_template = last_unknown_template.load(std::memory_order_relaxed);
        return s;
    }

    std::string to_json() const {
        const Snapshot s = snapshot();
        std::string o = "{";
#define X(n) o += "\"" #n "\":" + std::to_string(s.n) + ",";
        SUBPLANE_PARTITION_COUNTERS(X)
#undef X
        o += "\"last_unknown_template\":" + std::to_string(s.last_unknown_template) + "}";
        return o;
    }
};

// ring_overruns: the writer cannot observe reader overruns; kept for future
// use (e.g. reader-reported via control channel).
struct alignas(64) SessionCounters {
    struct Snapshot {
#define X(n) uint64_t n = 0;
        SUBPLANE_SESSION_COUNTERS(X)
#undef X
    };

#define X(n) std::atomic<uint64_t> n{0};
    SUBPLANE_SESSION_COUNTERS(X)
#undef X

    Snapshot snapshot() const {
        Snapshot s;
#define X(n) s.n = n.load(std::memory_order_relaxed);
        SUBPLANE_SESSION_COUNTERS(X)
#undef X
        return s;
    }

    std::string to_json() const {
        const Snapshot s = snapshot();
        std::string o = "{";
        bool first = true;
#define X(n) o += (first ? "" : ","); first = false; o += "\"" #n "\":" + std::to_string(s.n);
        SUBPLANE_SESSION_COUNTERS(X)
#undef X
        o += "}";
        return o;
    }
};

#undef SUBPLANE_PARTITION_COUNTERS
#undef SUBPLANE_SESSION_COUNTERS

} // namespace subplane
