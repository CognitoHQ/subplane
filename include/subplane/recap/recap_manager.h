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
// `RecapRecord`s, `RecapEnd`, sent contiguously (replies to different requests
// are not interleaved); `RecapReject`, or a non-Ok `Status`, replaces the whole
// reply. A reply whose `RecapBegin` names its Instrument is a *section*: it is
// matched by instrument, not by request, so a section published once on the
// service's shared section stream serves every agent waiting on that
// instrument, whoever asked. One without an Instrument is matched by
// CorrelationId (a service that answers 1:1). In `reply: stream_markers` mode
// the same shape comes in the stream's own schema and framing instead, between
// decoder-mapped RECAP_BEGIN / RECAP_END / RECAP_REJECT markers.
//
// Resolver: a topic may name a resolver (`resolver:`), a service answering SPP
// `Resolve`. The manager runs its client on this thread too: the whole topic is
// asked for on every connect and each refresh_ms; a complete answer is published
// to the topic's TopicDirectory (session names) and noted to the pipeline
// (homes, the topic check).
//
// Bulk: a REQUEST marked bulk (a partition-wide resync: a gap, a sequence
// reset, a publisher restart) joins one `RecapRequest{Bulk}` for the whole
// partition, answered by sections. It times out on silence: every section the
// service publishes while it is out pushes the deadline back, so a long paced
// burst never expires while it is still arriving.

#include "subplane/core/spsc_queue.h"
#include "subplane/recap/recap_config.h"
#include "subplane/recap/recap_transport.h"
#include "subplane/recap/topic_directory.h"

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

// INTENT: `generation` is the pipeline's recap generation for the partition
// (RecapState::generation). It rises when the partition's stream changes under
// the pending recaps: a gap, a sequence reset, a publisher restart. A REQUEST
// with a newer generation than the symbol's request renews it: the request in
// flight is abandoned (its reply becomes stray) and a fresh one goes out at
// once. Every reply carries the generation of its request, and the pipeline
// drops one from an older generation, which covers a reply already queued
// when the renew arrived. Coalescing into the old request instead would merge
// a snapshot of the stream as it was before the change.
//
// `floor` is the pending symbol's floor (RecapPending::floor) when posted: a
// section older than it cannot be merged, so the manager skips it instead of
// filling a slab the pipeline would refuse (it refuses it anyway).
struct RecapCommand {
    // FLOOR: the symbol's floor moved (the pipeline rebased it); `floor` is the new one.
    enum class Op : uint8_t { REQUEST, CANCEL, FLOOR };
    Op       op = Op::REQUEST;
    uint16_t partition = 0;
    uint32_t symbol_id = 0;
    uint32_t generation = 0;
    uint64_t floor = 0;
    bool     bulk = false;
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
    uint32_t       generation = 0;     // RecapCommand::generation of the request
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

// What a topic's resolver said, for the pipeline thread: the topic (checked
// against the config) and each instrument's home partition (seeds inst_home,
// so recap for an instrument not yet seen asks its home only).
struct ResolveNote {
    enum class Kind : uint8_t { TOPIC, HOME };
    Kind     kind = Kind::HOME;
    bool     mismatch = false;   // TOPIC: the resolver's topic id or partition count differs from the config
    uint16_t partition = 0;      // Partition::id() of the topic's partition 1
    uint16_t topic_id = 0;       // TOPIC: as the resolver has it
    uint16_t partitions = 0;     // TOPIC: as the resolver has it
    uint16_t home_num = 0;       // HOME: the instrument's partition_num
    uint32_t symbol_id = 0;      // HOME
};

struct RecapChannel {
    SpscQueue<RecapCommand, 4096> requests;   // pipeline -> manager
    SpscQueue<RecapReply, 1024>   replies;    // manager -> pipeline
    SpscQueue<RecapRelease, 1024> releases;   // pipeline -> manager
    SpscQueue<ResolveNote, 4096>  resolved;   // manager -> pipeline
};

class RecapManager {
public:
    RecapManager(uint64_t epoch, SymbolTable& symbols);
    ~RecapManager();

    // Cold, before open(). The partition must have a recap block; its
    // component is taken from it. One channel may serve several partitions.
    bool add_partition(Partition& p, RecapChannel& ch, std::string& err);
    // Cold, before open(). `p` is the topic's partition 1 (it holds the
    // resolver block); `partitions` is how many the config gives the topic.
    // Each complete answer is published to `dir` and noted to `ch`.
    bool add_resolver(Partition& p, RecapChannel& ch, TopicDirectory& dir, uint16_t partitions, std::string& err);
    bool open(std::string& err);           // opens every component
    void start();
    void stop();
    size_t partitions() const noexcept { return parts_.size(); }
    size_t resolvers() const noexcept { return resolvers_.size(); }

    // One iteration (the thread loop, or a test with its own clock). Returns
    // work done (commands, fragments, sends, timeouts).
    int run_once(uint64_t now_mono_ns) noexcept;

private:
    struct Entry {
        enum class State : uint8_t { QUEUED, INFLIGHT, BACKOFF };
        State    state = State::QUEUED;
        bool     cancelled = false;
        bool     bulk = false;   // waits on the partition's bulk request, not a request of its own
        uint32_t attempt = 0;
        uint32_t generation = 0;
        uint64_t request_id = 0;
        uint64_t due_ns = 0;     // QUEUED: not sent before (coalesce_ms); INFLIGHT: timeout (single); BACKOFF: next try
        uint64_t floor = 0;      // RecapCommand::floor
    };
    // INTENT: one per source. A section on the shared stream and a reply on
    // the response channel can be in progress at once; each source delivers
    // its own messages in order, so neither interleaves with itself.
    struct Assembly {
        bool     active = false;     // between BEGIN and END
        bool     skipping = false;   // between an unknown BEGIN and its END
        bool     overflow = false;
        uint64_t request_id = 0;     // the reply's CorrelationId (a section's: its section id)
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
        std::deque<uint32_t> fifo;                        // single symbols waiting to be sent
        uint32_t inflight = 0;                            // single requests in flight (max_inflight)
        uint32_t bulk_queued = 0, bulk_inflight = 0;      // entries waiting on a bulk request, before / after it is sent
        uint64_t bulk_due = 0;                            // QUEUED: gather until; INFLIGHT: silence timeout
        uint64_t bulk_rid = 0;                            // the bulk request in flight, 0 if none
        Assembly asm_[kRecapSources];
        Peer     peer;
        bool     hello_sent = false;   // our Hello went out on this connection
        bool     was_connected = false;
        uint8_t  tx[256] = {};         // one outgoing frame; every message the manager sends fits
        uint64_t now = 0;
        uint64_t last_overflow_log_ns = 0;
        void on_fragment(const uint8_t* data, uint32_t len, RecapSource src) noexcept override;
    };
    // A topic's resolver client (SPP Resolve): asks for the whole topic on
    // every connect and each refresh_ms, assembles the paged answer.
    struct RState : RecapFragmentSink {
        RecapManager*   mgr = nullptr;
        Partition*      part = nullptr;     // the topic's partition 1
        RecapChannel*   ch = nullptr;
        TopicDirectory* dir = nullptr;
        uint16_t        configured = 0;     // the topic's partition count in the config
        std::unique_ptr<RecapTransport> transport;
        ResolverConfig  cfg;
        Peer     peer;
        bool     hello_sent = false;
        bool     was_connected = false;
        uint8_t  tx[256] = {};
        uint64_t now = 0;
        uint64_t rid = 0;                   // the Resolve in flight, 0 if none
        uint64_t due_ns = 0;                // rid != 0: its timeout; else: the next ask
        uint16_t got_topic = 0, got_parts = 0;
        std::vector<ResolvedInstrument> building;
        bool     noted = false;             // a TOPIC note went out
        uint16_t noted_topic = 0, noted_parts = 0;
        std::unordered_map<uint32_t, uint16_t> noted_home;   // symbol_id -> partition_num sent
        void on_fragment(const uint8_t* data, uint32_t len, RecapSource src) noexcept override;
    };
    struct BeginInfo {
        bool     has_id = false, has_as_of = false, has_epoch = false;
        uint64_t rid = 0, as_of = 0, epoch = 0;
        uint64_t instrument = 0;     // a section; 0 = a reply matched by rid
        uint16_t venue = 0;
    };

    void on_marker_message(PState& ps, const uint8_t* msg, uint32_t len) noexcept;  // reply: stream_markers
    void on_spp_message(PState& ps, RecapSource src, const uint8_t* msg, uint32_t len) noexcept;     // reply: spp
    void reply_begin(PState& ps, RecapSource src, const BeginInfo& b) noexcept;
    void reply_record(PState& ps, RecapSource src, const uint8_t* msg, uint32_t len) noexcept;
    void reply_end(PState& ps, RecapSource src, bool has_id, uint64_t rid) noexcept;
    void reply_reject(PState& ps, bool has_id, uint64_t rid, uint64_t reason) noexcept;
    uint32_t section_symbol(const PState& ps, uint64_t instrument, uint16_t venue) const noexcept;
    void send_bulk(PState& ps) noexcept;
    void reset_bulk(PState& ps) noexcept;   // the bulk in flight is abandoned; its entries wait for the next one
    void on_peer_hello(PState& ps, const uint8_t* msg, uint32_t len) noexcept;
    bool send_hello(PState& ps) noexcept;    // false when back-pressured
    void send_status(PState& ps, uint64_t correlation_id, uint16_t code) noexcept;
    RecapSendStatus send_frame(PState& ps, uint32_t msg_len) noexcept; // ps.tx[4..] holds the message
    void handle_command(PState& ps, const RecapCommand& c) noexcept;
    void renew(PState& ps, uint32_t symbol_id, Entry& e, uint32_t generation, bool bulk) noexcept;
    void send_ready(PState& ps) noexcept;
    void service_timeouts(PState& ps) noexcept;
    void finish(PState& ps, uint32_t symbol_id, bool success) noexcept;   // request over: retry or forget
    void fail(PState& ps, uint32_t symbol_id, RecapStatus st, uint64_t reason) noexcept;
    void abort_assembly(PState& ps, RecapSource src) noexcept;
    void abort_assembly_of(PState& ps, uint32_t symbol_id) noexcept;   // any source assembling for the symbol
    void push_reply(PState& ps, const RecapReply& r) noexcept;
    void flush_outbox() noexcept;
    uint64_t backoff_ns(const PState& ps, uint32_t attempt) noexcept;
    void publish_gauges(PState& ps) noexcept;
    PState* find(uint16_t partition) noexcept;
    void loop() noexcept;
    void run_resolver(RState& rs) noexcept;
    void on_resolver_message(RState& rs, const uint8_t* msg, uint32_t len) noexcept;
    void resolver_complete(RState& rs) noexcept;
    void push_note(RecapChannel& ch, const ResolveNote& n) noexcept;

    uint64_t epoch_;
    SymbolTable& symbols_;
    uint64_t next_request_id_ = 1;
    uint64_t rng_;
    std::vector<std::unique_ptr<PState>> parts_;
    std::vector<std::unique_ptr<RState>> resolvers_;
    std::vector<RecapChannel*> channels_;
    std::deque<std::pair<RecapChannel*, RecapReply>> outbox_; // replies a full queue could not take yet
    std::deque<std::pair<RecapChannel*, ResolveNote>> note_outbox_;
    std::unordered_map<RecapChannel*, uint32_t> note_waiting_;   // notes of each channel in note_outbox_
    std::thread thread_;
    std::atomic<bool> stop_{false};
    bool running_ = false;
};

} // namespace subplane
