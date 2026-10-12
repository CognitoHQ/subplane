// views_app: read a flashtick ring with the generated canonical views, no codec.
//
//   views_app <ring path> [--count N] [--seconds N] [--quiet] [--from-start]
//
// Dispatches every data record through ft_canonical::view<T>() (a pointer into
// the ring, checked against length, template id and schema id), prints a line
// per message unless --quiet, and ends with one summary line:
//   trades=N quotes=N books=N snapshots=N other=N heartbeats=N bad=N
// `bad` counts data records that no view accepted (wrong schema, short payload).
// Depends only on the client headers and the generated flashtick_canonical_views.h.

#include "flashtick/client/shm_reader.h"
#include "flashtick/core/clock.h"
#include "flashtick_canonical_views.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

using namespace flashtick;
using namespace ft_canonical;

namespace {
double px(int64_t v) { return static_cast<double>(v) / 1e9; } // PriceExponent = -9
}

int main(int argc, char** argv) {
    if (argc < 2) { std::fputs("usage: views_app <ring path> [--count N] [--seconds N] [--quiet] [--from-start]\n", stderr); return 2; }
    const std::string path = argv[1];
    uint64_t count = 0, seconds = 0; bool quiet = false, from_start = false;
    for (int i = 2; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--quiet") quiet = true;
        else if (a == "--from-start") from_start = true;
        else if ((a == "--count" || a == "--seconds") && i + 1 < argc) { (a == "--count" ? count : seconds) = std::strtoull(argv[++i], nullptr, 10); }
        else { std::fprintf(stderr, "views_app: bad argument %s\n", a.c_str()); return 2; }
    }
    ShmReader rd;
    std::string err;
    if (!rd.open(path, &err)) { std::fprintf(stderr, "views_app: %s\n", err.c_str()); return 1; }
    if (from_start) rd.seek(0);

    uint64_t trades = 0, quotes = 0, books = 0, snapshots = 0, other = 0, heartbeats = 0, bad = 0, seen = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    RecordView rv;
    while (count == 0 || seen < count) {
        const ReaderStatus st = rd.poll(rv);
        if (st == ReaderStatus::Empty) {
            if (seconds && std::chrono::steady_clock::now() >= deadline) break;
            std::this_thread::sleep_for(std::chrono::microseconds(200));
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
    return bad ? 1 : 0;
}
