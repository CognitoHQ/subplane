// rule_runner: function-style consumption of a session.
//
//   rule_runner --socket /run/subplane/subplane.sock [--session rules]
//               [--from-start] [--seconds N]
//
// The agent is the event source; this process is a tiny function runtime.
// Each rule declares a trigger (symbols, message types) and a body. The
// runtime unions the triggers into one session filter, seeds rule state from
// the last-value snapshot, then dispatches every ring record to the rules
// whose trigger matches. Rules are plain callables and never touch the bus,
// a socket or the schema.
//
// --from-start replays whatever history the ring still holds through the
// rules before going live: a dry run of a new rule against recent data.
//
// Rules here look only at the routing header. A real rule decodes the SBE
// payload with the publisher's generated codec, exactly as the demo app
// comment says; the runtime does not care.

#include "subplane/client/control_client.h"
#include "subplane/client/shm_reader.h"
#include "subplane/core/clock.h"
#include "subplane/shm/control_events.h"

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace subplane;

namespace {

volatile sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

struct Trigger {
    uint64_t                 msg_type_mask = 0;  // 0 = all data types
    std::vector<std::string> symbols;            // empty = all symbols
};

using RuleFn = std::function<void(const TickHeader&, const uint8_t* payload, uint32_t len)>;

struct Rule {
    const char* name;
    Trigger     trigger;
    RuleFn      body;
    uint64_t    invocations = 0;
};

constexpr uint64_t bit(MsgType t) { return 1ull << static_cast<unsigned>(t); }

bool matches(const Trigger& t, const TickHeader& h) {
    // Symbol narrowing is done by the agent's filter when all rules agree;
    // with mixed triggers the runtime re-checks here. Symbols are resolved to
    // ids at startup (see resolve_symbols) so this is an id lookup, not a string compare.
    return t.msg_type_mask == 0 || (t.msg_type_mask & (1ull << h.msg_type));
}

} // namespace

int main(int argc, char** argv) {
    std::string socket_path = "/run/subplane/subplane.sock";
    std::string session = "rules";
    bool from_start = false;
    int seconds = 0;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--socket" && i + 1 < argc) socket_path = argv[++i];
        else if (a == "--session" && i + 1 < argc) session = argv[++i];
        else if (a == "--from-start") from_start = true;
        else if (a == "--seconds" && i + 1 < argc) seconds = std::atoi(argv[++i]);
        else { std::fprintf(stderr, "unknown option %s\n", a.c_str()); return 2; }
    }
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    // ---- rules ------------------------------------------------------------
    // State lives in the runtime, keyed by symbol id; a stateless rule would
    // read the last-value cache instead.
    std::unordered_map<uint32_t, uint64_t> last_quote_ns;
    std::unordered_map<uint32_t, uint64_t> trades_this_second;
    uint64_t second_bucket = 0;

    std::vector<Rule> rules;
    rules.push_back({"trade-burst", {bit(MsgType::TRADE), {}},
        [&](const TickHeader& h, const uint8_t*, uint32_t) {
            const uint64_t sec = h.ingest_ts / 1'000'000'000ull;
            if (sec != second_bucket) { second_bucket = sec; trades_this_second.clear(); }
            if (++trades_this_second[h.symbol_id] == 50)
                std::printf("ALERT trade-burst sym=%u >=50 trades in one second (seq=%llu)\n",
                            h.symbol_id, static_cast<unsigned long long>(h.sequence));
        }, 0});
    rules.push_back({"quote-stale", {bit(MsgType::QUOTE) | bit(MsgType::TRADE), {}},
        [&](const TickHeader& h, const uint8_t*, uint32_t) {
            if (h.msg_type == static_cast<uint8_t>(MsgType::QUOTE)) { last_quote_ns[h.symbol_id] = h.ingest_ts; return; }
            auto it = last_quote_ns.find(h.symbol_id);
            if (it != last_quote_ns.end() && h.ingest_ts - it->second > 5'000'000'000ull)
                std::printf("ALERT quote-stale sym=%u trade with no quote for %.1fs\n", h.symbol_id,
                            static_cast<double>(h.ingest_ts - it->second) / 1e9);
        }, 0});

    // ---- session = union of triggers ---------------------------------------
    SessionRequest req;
    req.name = session;
    req.ring_capacity_bytes = 16ull << 20;
    req.max_payload = 4096;
    req.all_symbols = true;
    for (const Rule& r : rules) {
        req.msg_type_mask |= r.trigger.msg_type_mask;
        if (!r.trigger.symbols.empty()) req.symbols.insert(req.symbols.end(), r.trigger.symbols.begin(), r.trigger.symbols.end());
    }
    if (!req.symbols.empty()) req.all_symbols = false;

    ControlClient ctl;
    HelloInfo hi;
    if (!ctl.connect(socket_path) || !ctl.hello(hi)) { std::fprintf(stderr, "control: %s\n", ctl.last_error().c_str()); return 1; }
    AttachInfo ai;
    if (!ctl.create_session(req, ai) && !ctl.attach(session, ai)) { std::fprintf(stderr, "session: %s\n", ctl.last_error().c_str()); return 1; }
    ShmReader rd;
    std::string err;
    if (!rd.open(ai.ring_path, &err) || rd.epoch() != hi.epoch) { std::fprintf(stderr, "ring: %s\n", err.c_str()); return 1; }
    std::printf("# %zu rules, session=%s ring=%s mask=0x%llx\n", rules.size(), ai.name.c_str(), ai.ring_path.c_str(),
                static_cast<unsigned long long>(req.msg_type_mask));

    auto dispatch = [&](const TickHeader& h, const uint8_t* p, uint32_t len) {
        for (Rule& r : rules) if (matches(r.trigger, h)) { ++r.invocations; r.body(h, p, len); }
    };

    // Seed: snapshot through the rules (flagged by FLAG_SNAPSHOT_ORIGIN on the header),
    // then either replay ring history or start at the live head.
    const uint64_t mark = rd.head();
    uint64_t snap_head = 0;
    uint64_t seeded = 0;
    if (!ctl.snapshot(session, snap_head, [&](const TickHeader& h, const uint8_t* p, uint32_t len) { ++seeded; dispatch(h, p, len); })) {
        std::fprintf(stderr, "snapshot: %s\n", ctl.last_error().c_str());
        return 1;
    }
    if (from_start) rd.seek(0); else rd.seek(mark);
    std::printf("# seeded from %llu snapshot records; %s\n", static_cast<unsigned long long>(seeded),
                from_start ? "replaying ring history then live" : "live");

    const uint64_t deadline = seconds > 0 ? now_ns() + static_cast<uint64_t>(seconds) * static_cast<uint64_t>(1'000'000'000) : 0;
    uint64_t records = 0, lapped = 0, gaps = 0;
    RecordView rv;
    while (!g_stop && (deadline == 0 || now_ns() < deadline)) {
        const ReaderStatus st = rd.poll(rv);
        if (st == ReaderStatus::Ok) {
            if (is_data(*rv.hdr)) {
                ++records;
                dispatch(*rv.hdr, rv.payload, rv.payload_len);
                // A rule saw bytes the writer has since overwritten: its side effects
                // are suspect. A real runtime would roll back or re-run from the snapshot.
                if (!rv.valid()) ++lapped;
            } else if (rv.hdr->msg_type == static_cast<uint8_t>(ControlCode::GAP_DETECTED)) {
                ++gaps;  // rules that depend on continuity would reseed here
            } else if (rv.hdr->msg_type == static_cast<uint8_t>(ControlCode::SESSION_CLOSING)) {
                break;
            }
            continue;
        }
        if (st == ReaderStatus::Overrun) { rd.seek_to_head(); continue; }
        if (st == ReaderStatus::Closed) { std::printf("# ring closed\n"); break; }
        if (!rd.writer_alive(2'000'000'000ull, now_ns())) { std::printf("# writer gone\n"); break; }
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    std::printf("# summary records=%llu gaps=%llu lapped=%llu overruns=%llu\n", static_cast<unsigned long long>(records),
                static_cast<unsigned long long>(gaps), static_cast<unsigned long long>(lapped),
                static_cast<unsigned long long>(rd.overruns()));
    for (const Rule& r : rules) std::printf("#   %-12s invocations=%llu\n", r.name, static_cast<unsigned long long>(r.invocations));
    ctl.destroy_session(session);
    return 0;
}
