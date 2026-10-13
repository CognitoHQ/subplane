// subplane aeron-pub: canonical-schema publisher over Aeron, the proof and the
// test peer for the `aeron` transport. One SBE message per offer (Trade, Quote
// alternating, a Heartbeat every --heartbeat-every messages), all sharing one
// Sequence counter so the agent's framing can track gaps with
// `sequence: message.Sequence`. Messages are built from the schema at runtime
// (no generated header), so the CLI does not depend on its own `schema` output.
//
//   subplane aeron-pub --channel aeron:ipc --stream-id 1001 [--aeron-dir DIR] [--schema X.xml] [--count N]
//                      [--rate msg/s] [--symbols N] [--heartbeat-every N] [--embedded-driver] [--driver-timeout-ms N]
//
// --embedded-driver starts a media driver in this process (its own threads) so
// a UDP test needs no aeronmd; the IPC case attaches to the agent's driver dir.

#include "commands.h"
#include "sbe_encoder.h"
#include "tool_common.h"

#include <aeronc.h>
#include <aeronmd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sched.h>
#include <string>
#include <thread>

#ifndef SP_CANONICAL_SCHEMA
#define SP_CANONICAL_SCHEMA "schemas/subplane_canonical.xml"
#endif

namespace {

using namespace subplane;

uint64_t now_ns() { return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count()); }

int fail(const char* what) { std::fprintf(stderr, "subplane aeron-pub: %s: %s\n", what, aeron_errmsg()); return 1; }

void usage() {
    std::fputs("usage: subplane aeron-pub --channel aeron:ipc --stream-id 1001 [--aeron-dir DIR] [--schema X.xml] [--count N]\n"
               "                          [--rate msg/s] [--symbols N] [--heartbeat-every N] [--embedded-driver] [--driver-timeout-ms N]\n", stderr);
}

// Offers until accepted; back pressure and admin actions retry, anything else is fatal.
bool offer(aeron_exclusive_publication_t* pub, const uint8_t* msg, size_t len, uint64_t timeout_ns) {
    const auto start = std::chrono::steady_clock::now();
    for (;;) {
        const int64_t r = aeron_exclusive_publication_offer(pub, msg, len, nullptr, nullptr);
        if (r >= 0) return true;
        if (r == AERON_PUBLICATION_CLOSED || r == AERON_PUBLICATION_ERROR || r == AERON_PUBLICATION_MAX_POSITION_EXCEEDED) return false;
        if (std::chrono::steady_clock::now() - start > std::chrono::nanoseconds(timeout_ns)) return false;
        sched_yield(); // BACK_PRESSURED, ADMIN_ACTION, NOT_CONNECTED (subscriber not up yet)
    }
}

// Canonical prefix shared by every data message.
void prefix(const SbeSchema& s, uint8_t* msg, const MessageDef& m, uint32_t instrument, uint64_t seq) {
    const uint64_t ts = now_ns();
    sptool::set_u64(s, msg, m, "InstrumentId", instrument);
    sptool::set_u64(s, msg, m, "VenueId", 1);
    sptool::set_u64(s, msg, m, "Sequence", seq);
    sptool::set_u64(s, msg, m, "ExchangeTs", ts);
    sptool::set_u64(s, msg, m, "RecvTs", ts);
}

} // namespace

int cmd_aeron_pub(int argc, char** argv) {
    sptool::Args args;
    std::string err;
    if (!args.parse(argc, argv, {"channel", "stream-id", "aeron-dir", "schema", "count", "rate", "symbols", "heartbeat-every", "driver-timeout-ms"},
                    {"embedded-driver", "help"}, err)) {
        std::fprintf(stderr, "subplane aeron-pub: %s\n", err.c_str()); usage(); return 2;
    }
    if (args.has("help")) { usage(); return 0; }
    const std::string channel = args.str("channel", "aeron:ipc"), dir = args.str("aeron-dir", "");
    uint64_t stream_id = 0, count = 0, rate = 0, symbols = 0, hb_every = 0, driver_timeout_ms = 0;
    if (!args.u64("stream-id", 1001, stream_id, err) || !args.u64("count", 0, count, err) || !args.u64("rate", 1000, rate, err) ||
        !args.u64("symbols", 5, symbols, err) || !args.u64("heartbeat-every", 100, hb_every, err) ||
        !args.u64("driver-timeout-ms", 10000, driver_timeout_ms, err)) {
        std::fprintf(stderr, "subplane aeron-pub: %s\n", err.c_str()); return 2;
    }
    const bool embedded = args.has("embedded-driver");
    if (symbols == 0) { std::fputs("subplane aeron-pub: --symbols must be > 0\n", stderr); return 2; }

    sptool::LoadedSchema ls;
    if (!ls.load(args.str("schema", SP_CANONICAL_SCHEMA), err)) { std::fprintf(stderr, "subplane aeron-pub: schema: %s\n", err.c_str()); return 2; }
    const SbeSchema& s = ls.schema;
    const MessageDef* trade = s.find_message(1);
    const MessageDef* quote = s.find_message(2);
    const MessageDef* hb = s.find_message(100);
    if (!trade || !quote || !hb) { std::fputs("subplane aeron-pub: schema needs templates 1 (Trade), 2 (Quote) and 100 (Heartbeat)\n", stderr); return 2; }

    aeron_driver_context_t* dctx = nullptr;
    aeron_driver_t* driver = nullptr;
    if (embedded) {
        if (aeron_driver_context_init(&dctx) < 0) return fail("driver context");
        if (!dir.empty() && aeron_driver_context_set_dir(dctx, dir.c_str()) < 0) return fail("driver dir");
        aeron_driver_context_set_threading_mode(dctx, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(dctx, true);
        aeron_driver_context_set_dir_delete_on_shutdown(dctx, true);
        if (aeron_driver_init(&driver, dctx) < 0) return fail("driver init");
        if (aeron_driver_start(driver, false) < 0) return fail("driver start");
    }

    aeron_context_t* ctx = nullptr;
    aeron_t* client = nullptr;
    if (aeron_context_init(&ctx) < 0) return fail("context");
    if (!dir.empty() && aeron_context_set_dir(ctx, dir.c_str()) < 0) return fail("dir");
    aeron_context_set_driver_timeout_ms(ctx, driver_timeout_ms);
    aeron_context_set_client_name(ctx, "subplane aeron-pub");
    if (aeron_init(&client, ctx) < 0) return fail("init (is the media driver running?)");
    if (aeron_start(client) < 0) return fail("start");

    aeron_async_add_exclusive_publication_t* async = nullptr;
    aeron_exclusive_publication_t* pub = nullptr;
    if (aeron_async_add_exclusive_publication(&async, client, channel.c_str(), static_cast<int32_t>(stream_id)) < 0) return fail("add publication");
    while (!pub) {
        if (aeron_async_add_exclusive_publication_poll(&pub, async) < 0) return fail("publication");
        sched_yield();
    }
    // Wait for the subscriber (the agent) so the first offers are not dropped as NOT_CONNECTED.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(driver_timeout_ms);
    while (!aeron_exclusive_publication_is_connected(pub)) {
        if (std::chrono::steady_clock::now() > deadline) { std::fputs("subplane aeron-pub: no subscriber connected\n", stderr); return 1; }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    const uint64_t offer_timeout_ns = 5'000'000'000ull;
    const auto interval = std::chrono::nanoseconds(rate ? 1'000'000'000ull / rate : 0);
    auto next = std::chrono::steady_clock::now();
    uint8_t buf[1024];
    sptool::MessageBuilder b(s);
    uint64_t seq = 0, sent = 0, heartbeats = 0;
    while (count == 0 || sent < count) {
        const uint32_t sym = static_cast<uint32_t>(1 + sent % symbols);
        const int64_t px = 100'000'000'000ll + static_cast<int64_t>(sym) * 1'000'000'000ll + static_cast<int64_t>(sent % 50) * 10'000'000ll;
        if (sent % 2 == 0) {
            b.begin(*trade, buf, sizeof buf);
            prefix(s, buf, *trade, sym, ++seq);
            sptool::set_u64(s, buf, *trade, "Price", static_cast<uint64_t>(px));
            sptool::set_u64(s, buf, *trade, "Qty", 250'000'000);
            sptool::set_u64(s, buf, *trade, "TradeId", seq);
            sptool::set_u64(s, buf, *trade, "Aggressor", 1 + seq % 2);
        } else {
            b.begin(*quote, buf, sizeof buf);
            prefix(s, buf, *quote, sym, ++seq);
            sptool::set_u64(s, buf, *quote, "BidPx", static_cast<uint64_t>(px - 10'000'000));
            sptool::set_u64(s, buf, *quote, "BidQty", 1'000'000'000);
            sptool::set_u64(s, buf, *quote, "AskPx", static_cast<uint64_t>(px + 10'000'000));
            sptool::set_u64(s, buf, *quote, "AskQty", 2'000'000'000);
        }
        if (!b.ok() || !offer(pub, buf, b.size(), offer_timeout_ns)) {
            std::fprintf(stderr, "subplane aeron-pub: offer failed after %llu messages\n", static_cast<unsigned long long>(sent)); return 1;
        }
        ++sent;
        if (hb_every && sent % hb_every == 0) {
            b.begin(*hb, buf, sizeof buf);
            prefix(s, buf, *hb, 0, ++seq);
            if (!b.ok() || !offer(pub, buf, b.size(), offer_timeout_ns)) { std::fputs("subplane aeron-pub: heartbeat offer failed\n", stderr); return 1; }
            ++heartbeats;
        }
        if (interval.count()) { next += interval; std::this_thread::sleep_until(next); }
    }
    std::printf("sent %llu messages and %llu heartbeats on %s stream %ld\n",
                static_cast<unsigned long long>(sent), static_cast<unsigned long long>(heartbeats), channel.c_str(), static_cast<long>(stream_id));
    std::fflush(stdout);
    // Let the driver's sender flush over UDP before the publication goes away.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    aeron_exclusive_publication_close(pub, nullptr, nullptr);
    aeron_close(client);
    aeron_context_close(ctx);
    if (driver) { aeron_driver_close(driver); aeron_driver_context_close(dctx); }
    return 0;
}
