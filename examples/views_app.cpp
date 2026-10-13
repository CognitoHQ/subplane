// views_app: read a subplane ring with the generated canonical views, no codec.
//
//   views_app <ring path> [--count N] [--seconds N] [--quiet] [--from-start] [--spin] [--latency]
//
// Dispatches every data record through sp_canonical::view<T>() (a pointer into
// the ring, checked against length, template id and schema id), prints a line
// per message unless --quiet, and ends with one summary line:
//   trades=N quotes=N books=N snapshots=N other=N heartbeats=N bad=N
// `bad` counts data records that no view accepted (wrong schema, short payload).
// --spin busy-polls the ring instead of sleeping 200 us when it is empty.
// --latency adds a line of histograms (CLOCK_REALTIME on all three processes, one box):
//   gw_to_app: canonical RecvTs (the gateway's frame receive) to this read
//   agent_to_app: ring ingest_ts (the agent's packet receive) to this read
//   gw_to_agent: RecvTs to ingest_ts (Aeron hop plus the gateway's processing)
// Depends only on the client headers and the generated subplane_canonical_views.h.

#include "subplane/client/shm_reader.h"
#include "subplane/core/clock.h"
#include "subplane/core/histogram.h"
#include "subplane_canonical_views.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

using namespace subplane;
using namespace sp_canonical;

namespace {
double px(int64_t v) { return static_cast<double>(v) / 1e9; } // PriceExponent = -9
}

int main(int argc, char** argv) {
    if (argc < 2) { std::fputs("usage: views_app <ring path> [--count N] [--seconds N] [--quiet] [--from-start]\n", stderr); return 2; }
    const std::string path = argv[1];
    uint64_t count = 0, seconds = 0; bool quiet = false, from_start = false, spin = false, latency = false;
    for (int i = 2; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--quiet") quiet = true;
        else if (a == "--from-start") from_start = true;
        else if (a == "--spin") spin = true;
        else if (a == "--latency") latency = true;
        else if ((a == "--count" || a == "--seconds") && i + 1 < argc) { (a == "--count" ? count : seconds) = std::strtoull(argv[++i], nullptr, 10); }
        else { std::fprintf(stderr, "views_app: bad argument %s\n", a.c_str()); return 2; }
    }
    ShmReader rd;
    std::string err;
    if (!rd.open(path, &err)) { std::fprintf(stderr, "views_app: %s\n", err.c_str()); return 1; }
    if (from_start) rd.seek(0);

    uint64_t trades = 0, quotes = 0, books = 0, snapshots = 0, other = 0, heartbeats = 0, bad = 0, seen = 0, unordered = 0;
    LogHistogram gw_to_app, agent_to_app, gw_to_agent;   // static-sized, no allocation
    constexpr size_t kRecvTsOff = offsetof(Trade, RecvTs);  // the 32-byte prefix is the same on every canonical message
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    RecordView rv;
    while (count == 0 || seen < count) {
        const ReaderStatus st = rd.poll(rv);
        if (st == ReaderStatus::Empty) {
            if (seconds && std::chrono::steady_clock::now() >= deadline) break;
            if (!spin) std::this_thread::sleep_for(std::chrono::microseconds(200));
            continue;
        }
        if (st == ReaderStatus::Closed || st == ReaderStatus::WriterGone) break;
        if (st != ReaderStatus::Ok) continue; // Overrun: position resynced, keep going
        const TickHeader& h = *rv.hdr;
        if (h.kind == static_cast<uint8_t>(Kind::BUS_ADMIN)) {
            if (h.msg_type == static_cast<uint8_t>(ControlCode::BUS_HEARTBEAT)) ++heartbeats;
            continue;
        }
        if (h.kind != static_cast<uint8_t>(Kind::DATA)) continue;
        ++seen;
        if (latency && rv.payload_len >= kRecvTsOff + 8) {
            // INTENT: one clock read per record, taken before the views so the
            // measurement does not include this app's own decode.
            const uint64_t now = now_ns();
            uint64_t recv = 0; std::memcpy(&recv, rv.payload + kRecvTsOff, 8);
            if (recv && recv <= h.ingest_ts && h.ingest_ts <= now) {
                gw_to_app.record(now - recv); agent_to_app.record(now - h.ingest_ts); gw_to_agent.record(h.ingest_ts - recv);
            } else ++unordered;   // replayed recordings carry their original RecvTs; clocks of one box never run backwards
        }
        if (const Trade* t = view<Trade>(rv.payload, rv.payload_len)) {
            ++trades;
            if (!quiet) std::printf("trade  sym=%u seq=%llu px=%.9g qty=%.9g side=%u\n", t->InstrumentId, (unsigned long long)t->Sequence, px(t->Price), px(t->Qty), t->Aggressor);
        } else if (const Quote* q = view<Quote>(rv.payload, rv.payload_len)) {
            ++quotes;
            if (!quiet) std::printf("quote  sym=%u seq=%llu bid=%.9g x %.9g ask=%.9g x %.9g\n", q->InstrumentId, (unsigned long long)q->Sequence, px(q->BidPx), px(q->BidQty), px(q->AskPx), px(q->AskQty));
        } else if (const BookUpdate* b = view<BookUpdate>(rv.payload, rv.payload_len)) {
            ++books;
            if (!quiet) std::printf("book   sym=%u seq=%llu side=%u action=%u level=%u px=%.9g qty=%.9g\n", b->InstrumentId, (unsigned long long)b->Sequence, b->Side, b->Action, b->Level, px(b->Price), px(b->Qty));
        } else if (const BookSnapshot* s = view<BookSnapshot>(rv.payload, rv.payload_len)) {
            ++snapshots;
            if (!quiet) std::printf("snap   sym=%u depth=%u best bid=%.9g ask=%.9g\n", s->InstrumentId, s->Depth, px(s->BidPx[0]), px(s->AskPx[0]));
        } else if (view<Ohlcv>(rv.payload, rv.payload_len) || view<Status>(rv.payload, rv.payload_len) || view<InstrumentDefinition>(rv.payload, rv.payload_len)) {
            ++other;
        } else {
            ++bad;
        }
        // INTENT: the views point into the ring; a lap by the writer during the
        // reads above would have torn them, so the record is only counted if it
        // was still intact afterwards (the late-join idiom in the Client SDK docs).
        if (!rv.valid()) ++bad;
    }
    std::printf("trades=%llu quotes=%llu books=%llu snapshots=%llu other=%llu heartbeats=%llu bad=%llu\n",
                (unsigned long long)trades, (unsigned long long)quotes, (unsigned long long)books, (unsigned long long)snapshots,
                (unsigned long long)other, (unsigned long long)heartbeats, (unsigned long long)bad);
    if (latency) {
        auto line = [](const char* name, const LogHistogram& hgram) {
            const LogHistogram::Snapshot s = hgram.snapshot();
            std::printf("%s_ns: n=%llu p50=%llu p90=%llu p99=%llu p999=%llu max=%llu mean=%.0f\n", name, (unsigned long long)s.count,
                        (unsigned long long)percentile(s, 0.5), (unsigned long long)percentile(s, 0.9), (unsigned long long)percentile(s, 0.99),
                        (unsigned long long)percentile(s, 0.999), (unsigned long long)s.max, mean(s));
        };
        line("gw_to_app", gw_to_app); line("agent_to_app", agent_to_app); line("gw_to_agent", gw_to_agent);
        if (unordered) std::printf("latency_skipped=%llu (RecvTs missing or not ordered with ingest_ts and now)\n", (unsigned long long)unordered);
    }
    return bad ? 1 : 0;
}
