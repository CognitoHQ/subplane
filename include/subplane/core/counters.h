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
    X(gaps) X(dups) X(admin) X(degraded_events) X(decode_errors) X(malformed) X(truncated) X(transport_errors) X(seq_resets) \
    X(transport_down) X(transport_up) X(recap_markers) X(tap_records) X(tap_dropped) X(stream_mismatch) \
    X(inst_gaps) X(inst_dups)

// One writer per counter, as above: up to recap_sections_skipped by the recap
// manager thread, from recap_held on by the partition's pipeline thread.
#define SUBPLANE_RECAP_COUNTERS(X) \
    X(recap_requests) X(recap_coalesced) X(recap_replies) X(recap_rejects) X(recap_timeouts) \
    X(recap_overflows) X(recap_send_failed) X(recap_stray) X(recap_cancelled) X(recap_no_slab) X(recap_renewed) \
    X(recap_bulk_requests) X(recap_sections) X(recap_sections_skipped) \
    X(recap_held) X(recap_merged) X(recap_records) X(recap_too_old) X(recap_hold_overflows) X(recap_post_dropped) \
    X(recap_resyncs) X(recap_superseded) X(recap_elsewhere)

// Written by the recap manager thread, which runs the topic's resolver client.
#define SUBPLANE_RESOLVER_COUNTERS(X) \
    X(resolver_requests) X(resolver_replies) X(resolver_lists) X(resolver_rejects) X(resolver_timeouts) \
    X(resolver_stray) X(resolver_mismatch)

#define SUBPLANE_SESSION_COUNTERS(X) X(published) X(filtered) X(ring_overruns) X(publish_failed)

struct alignas(64) PartitionCounters {
    struct Snapshot {
#define X(n) uint64_t n = 0;
        SUBPLANE_PARTITION_COUNTERS(X)
#undef X
        uint32_t last_unknown_template = 0;
        uint32_t last_stream_stamp = 0;     // the stream_id of the last mismatching heartbeat
    };

#define X(n) std::atomic<uint64_t> n{0};
    SUBPLANE_PARTITION_COUNTERS(X)
#undef X
    std::atomic<uint32_t> last_unknown_template{0};
    std::atomic<uint32_t> last_stream_stamp{0};

    Snapshot snapshot() const {
        Snapshot s;
#define X(n) s.n = n.load(std::memory_order_relaxed);
        SUBPLANE_PARTITION_COUNTERS(X)
#undef X
        s.last_unknown_template = last_unknown_template.load(std::memory_order_relaxed);
        s.last_stream_stamp = last_stream_stamp.load(std::memory_order_relaxed);
        return s;
    }
    // Cold. f(name, value) for every counter, in declaration order.
    template <class F> static void each(const Snapshot& s, F&& f) {
#define X(n) f(#n, s.n);
        SUBPLANE_PARTITION_COUNTERS(X)
#undef X
    }

    std::string to_json() const {
        const Snapshot s = snapshot();
        std::string o = "{";
#define X(n) o += "\"" #n "\":" + std::to_string(s.n) + ",";
        SUBPLANE_PARTITION_COUNTERS(X)
#undef X
        o += "\"last_unknown_template\":" + std::to_string(s.last_unknown_template) +
             ",\"last_stream_stamp\":" + std::to_string(s.last_stream_stamp) + "}";
        return o;
    }
};

struct alignas(64) RecapCounters {
#define X(n) std::atomic<uint64_t> n{0};
    SUBPLANE_RECAP_COUNTERS(X)
#undef X
    std::atomic<uint64_t> inflight{0};  // gauge
    std::atomic<uint64_t> queued{0};    // gauge: waiting for an in-flight slot or a retry
    std::atomic<uint64_t> connected{0}; // gauge: 1 while the recap component is connected to its service

    // Cold. f(name, value) for every counter (not the gauges).
    template <class F> void each(F&& f) const {
#define X(n) f(#n, n.load(std::memory_order_relaxed));
        SUBPLANE_RECAP_COUNTERS(X)
#undef X
    }

    std::string to_json() const {
        std::string o = "{";
#define X(n) o += "\"" #n "\":" + std::to_string(n.load(std::memory_order_relaxed)) + ",";
        SUBPLANE_RECAP_COUNTERS(X)
#undef X
        o += "\"recap_inflight\":" + std::to_string(inflight.load(std::memory_order_relaxed)) +
             ",\"recap_queued\":" + std::to_string(queued.load(std::memory_order_relaxed)) + "}";
        return o;
    }
};

struct alignas(64) ResolverCounters {
#define X(n) std::atomic<uint64_t> n{0};
    SUBPLANE_RESOLVER_COUNTERS(X)
#undef X
    std::atomic<uint64_t> instruments{0}; // gauge: instruments in the last complete list
    std::atomic<uint64_t> connected{0};   // gauge: 1 while connected to the resolver

    // Cold. f(name, value) for every counter (not the gauges).
    template <class F> void each(F&& f) const {
#define X(n) f(#n, n.load(std::memory_order_relaxed));
        SUBPLANE_RESOLVER_COUNTERS(X)
#undef X
    }

    std::string to_json() const {
        std::string o = "{";
#define X(n) o += "\"" #n "\":" + std::to_string(n.load(std::memory_order_relaxed)) + ",";
        SUBPLANE_RESOLVER_COUNTERS(X)
#undef X
        o += "\"resolver_instruments\":" + std::to_string(instruments.load(std::memory_order_relaxed)) +
             ",\"resolver_connected\":" + std::to_string(connected.load(std::memory_order_relaxed)) + "}";
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
    template <class F> static void each(const Snapshot& s, F&& f) {
#define X(n) f(#n, s.n);
        SUBPLANE_SESSION_COUNTERS(X)
#undef X
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
