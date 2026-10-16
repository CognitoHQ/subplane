#pragma once

// RecapManager: the agent's one cold thread for recap. It takes requests from
// the pipelines, coalesces them per (partition, symbol), sends one SPP
// `RecapRequest` per symbol through the partition's recap component, assembles
// the reply into a preallocated slab, and hands the result back to the
// pipeline that asked. It speaks SPP (docs/protocol/subplane-protocol.md) to
// the snapshot service: `Hello` first on every connection, then requests.
//
// Threads: each pipeline group talks to the manager through one RecapChannel
// (three SPSC queues). The pipeline is the only producer of requests because
// it owns which sessions wait on a symbol; the manager is the only
// producer of replies. Nothing here runs on the pipeline thread.
//
// Reply contract with a snapshot service: a reply is `RecapBegin`, the
// `RecapRecord`s, `RecapEnd`, sent contiguously on the response stream
// (replies to different requests are not interleaved); `RecapReject`, or a
// non-Ok `Status`, replaces the whole reply. In `reply: stream_markers` mode
// the same shape comes in the stream's own schema and framing instead, between
// decoder-mapped RECAP_BEGIN / RECAP_END / RECAP_REJECT markers.

#include "subplane/core/spsc_queue.h"
#include "subplane/recap/recap_config.h"
#include "subplane/recap/recap_transport.h"

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace subplane {

class Partition;
class SymbolTable;

struct RecapCommand {
    enum class Op : uint8_t { REQUEST, CANCEL };
    Op       op = Op::REQUEST;
    uint16_t partition = 0;
    uint32_t symbol_id = 0;
};

enum class RecapStatus : uint8_t { OK, REJECTED, TIMEOUT, OVERFLOW, SEND_FAILED };
const char* recap_status_name(RecapStatus s) noexcept;

// One reply (or failure) for one request. On OK, `data` holds `messages`
// records, each a u32 little-endian length followed by the message bytes as
// the service sent them, and `slab` must be handed back through releases.
struct RecapReply {
    uint16_t       partition = 0;
    uint16_t       slab = 0;
    uint32_t       symbol_id = 0;
    uint64_t       request_id = 0;
    uint32_t       attempt = 0;        // 1 for the first request of this recap, +1 per retry
    RecapStatus    status = RecapStatus::OK;
    bool           has_source_epoch = false;
    uint64_t       as_of = 0;          // S: the sequence the snapshot is consistent with
    uint64_t       source_epoch = 0;
    uint64_t       reason = 0;         // REJECTED: RecapReject.Reason (or the marker's reason); 0x100 + the code for a Status
    const uint8_t* data = nullptr;     // OK only
    uint32_t       bytes = 0;
    uint32_t       messages = 0;
};

struct RecapRelease {
    uint16_t partition = 0;
    uint16_t slab = 0;
};

struct RecapChannel {
    SpscQueue<RecapCommand, 4096> requests;   // pipeline -> manager
    SpscQueue<RecapReply, 1024>   replies;    // manager -> pipeline
    SpscQueue<RecapRelease, 1024> releases;   // pipeline -> manager
};

class RecapManager {
public:
    RecapManager(uint64_t epoch, const SymbolTable& symbols);
    ~RecapManager();

    // Cold, before open(). The partition must have a recap block; its
    // component is taken from it. One channel may serve several partitions.
    bool add_partition(Partition& p, RecapChannel& ch, std::string& err);
    bool open(std::string& err);           // opens every component
    void start();
    void stop();
    size_t partitions() const noexcept { return parts_.size(); }

    // One iteration (the thread loop, or a test with its own clock). Returns
    // work done (commands, fragments, sends, timeouts).
    int run_once(uint64_t now_mono_ns) noexcept;

private:
    struct Entry {
        enum class State : uint8_t { QUEUED, INFLIGHT, BACKOFF };
        State    state = State::QUEUED;
        bool     cancelled = false;
        uint32_t attempt = 0;
        uint64_t request_id = 0;
        uint64_t due_ns = 0;     // INFLIGHT: timeout; BACKOFF: next try
    };
    struct Assembly {
        bool     active = false;     // between BEGIN and END
        bool     skipping = false;   // between an unknown BEGIN and its END
        bool     overflow = false;
        uint64_t request_id = 0;
        uint32_t symbol_id = 0;
        int      slab = -1;
        uint32_t used = 0;
        uint32_t messages = 0;
        uint64_t as_of = 0, source_epoch = 0;
        bool     has_source_epoch = false;
    };
    // The snapshot service as its last `Hello` described it (SPP section 5).
    // Reset on every reconnect: until a Hello arrives it is version 1.
    struct Peer {
        bool        hello = false;
        bool        incompatible = false; // its version range excludes ours: send nothing but Status
        uint16_t    version = 1;          // agreed: min(ours, theirs)
        uint64_t    epoch = 0;
        std::string service;
    };
    struct PState : RecapFragmentSink {
        RecapManager*   mgr = nullptr;
        Partition*      part = nullptr;
        RecapChannel*   ch = nullptr;
        std::unique_ptr<RecapTransport> transport;
        RecapConfig     cfg;
        std::vector<std::vector<uint8_t>> slabs;
        std::vector<uint16_t> free_slabs;
        std::unordered_map<uint32_t, Entry> entries;      // by symbol_id
        std::unordered_map<uint64_t, uint32_t> by_request; // request_id -> symbol_id
        std::deque<uint32_t> fifo;                        // symbols waiting to be sent
        uint32_t inflight = 0;
        Assembly asm_;
        Peer     peer;
        bool     hello_sent = false;   // our Hello went out on this connection
        bool     was_connected = false;
        uint8_t  tx[256] = {};         // one outgoing frame; every message the manager sends fits
        uint64_t now = 0;
        uint64_t last_overflow_log_ns = 0;
        void on_fragment(const uint8_t* data, uint32_t len) noexcept override;
    };

    void on_marker_message(PState& ps, const uint8_t* msg, uint32_t len) noexcept;  // reply: stream_markers
    void on_spp_message(PState& ps, const uint8_t* msg, uint32_t len) noexcept;     // reply: spp
    void reply_begin(PState& ps, bool has_id, uint64_t rid, bool has_as_of, uint64_t as_of, bool has_epoch, uint64_t epoch) noexcept;
    void reply_record(PState& ps, const uint8_t* msg, uint32_t len) noexcept;
    void reply_end(PState& ps, bool has_id, uint64_t rid) noexcept;
    void reply_reject(PState& ps, bool has_id, uint64_t rid, uint64_t reason) noexcept;
    void on_peer_hello(PState& ps, const uint8_t* msg, uint32_t len) noexcept;
    bool send_hello(PState& ps) noexcept;    // false when back-pressured
    void send_status(PState& ps, uint64_t correlation_id, uint16_t code) noexcept;
    RecapSendStatus send_frame(PState& ps, uint32_t msg_len) noexcept; // ps.tx[4..] holds the message
    void handle_command(PState& ps, const RecapCommand& c) noexcept;
    void send_ready(PState& ps) noexcept;
    void service_timeouts(PState& ps) noexcept;
    void finish(PState& ps, uint32_t symbol_id, bool success) noexcept;   // request over: retry or forget
    void fail(PState& ps, uint32_t symbol_id, RecapStatus st, uint64_t reason) noexcept;
    void abort_assembly(PState& ps) noexcept;
    void push_reply(PState& ps, const RecapReply& r) noexcept;
    void flush_outbox() noexcept;
    uint64_t backoff_ns(const PState& ps, uint32_t attempt) noexcept;
    void publish_gauges(PState& ps) noexcept;
    PState* find(uint16_t partition) noexcept;
    void loop() noexcept;

    uint64_t epoch_;
    const SymbolTable& symbols_;
    uint64_t next_request_id_ = 1;
    uint64_t rng_;
    std::vector<std::unique_ptr<PState>> parts_;
    std::vector<RecapChannel*> channels_;
    std::deque<std::pair<RecapChannel*, RecapReply>> outbox_; // replies a full queue could not take yet
    std::thread thread_;
    std::atomic<bool> stop_{false};
    bool running_ = false;
};

} // namespace subplane
