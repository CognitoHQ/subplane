// sp_aeron_pub: canonical-schema publisher over Aeron, the proof and the test
// peer for the `aeron` transport. One SBE message per offer (Trade, Quote
// alternating, a Heartbeat every --heartbeat-every messages), all sharing one
// Sequence counter so the agent's framing can track gaps with
// `sequence: message.Sequence`.
//
//   sp_aeron_pub --channel aeron:ipc --stream-id 1001 [--aeron-dir DIR] [--count N] [--rate msg/s]
//                [--symbols N] [--heartbeat-every N] [--embedded-driver] [--driver-timeout-ms N]
//
// --embedded-driver starts a media driver in this process (its own threads) so
// a UDP test needs no aeronmd; the IPC case attaches to the agent's driver dir.

#include "subplane_canonical_views.h"

#include <aeronc.h>
#include <aeronmd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sched.h>
#include <string>
#include <thread>

using namespace sp_canonical;

namespace {

uint64_t now_ns() { return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count()); }

template <class T>
void begin(T& m, uint32_t instrument, uint64_t seq) {
    std::memset(&m, 0, sizeof m);
    m.blockLength = static_cast<uint16_t>(T::kSize - kHeaderSize);
    m.templateId = T::kTemplateId;
    m.schemaId = kSchemaId;
    m.version = kSchemaVersion;
    m.InstrumentId = instrument;
    m.VenueId = 1;
    m.Sequence = seq;
    m.ExchangeTs = now_ns();
    m.RecvTs = m.ExchangeTs;
}

int fail(const char* what) { std::fprintf(stderr, "sp_aeron_pub: %s: %s\n", what, aeron_errmsg()); return 1; }

// Offers until accepted; back pressure and admin actions retry, anything else is fatal.
bool offer(aeron_exclusive_publication_t* pub, const void* msg, size_t len, uint64_t timeout_ns) {
    const auto start = std::chrono::steady_clock::now();
    for (;;) {
        const int64_t r = aeron_exclusive_publication_offer(pub, static_cast<const uint8_t*>(msg), len, nullptr, nullptr);
        if (r >= 0) return true;
        if (r == AERON_PUBLICATION_CLOSED || r == AERON_PUBLICATION_ERROR || r == AERON_PUBLICATION_MAX_POSITION_EXCEEDED) return false;
        if (std::chrono::steady_clock::now() - start > std::chrono::nanoseconds(timeout_ns)) return false;
        sched_yield(); // BACK_PRESSURED, ADMIN_ACTION, NOT_CONNECTED (subscriber not up yet)
    }
}

} // namespace

int main(int argc, char** argv) {
    std::string channel = "aeron:ipc", dir;
    long stream_id = 1001;
    uint64_t count = 0, rate = 1000, symbols = 5, hb_every = 100, driver_timeout_ms = 10000;
    bool embedded = false;
    for (int i = 1; i < argc; ++i) {
        const std::string k = argv[i];
        if (k == "--embedded-driver") { embedded = true; continue; }
        if (i + 1 >= argc) { std::fprintf(stderr, "sp_aeron_pub: %s needs a value\n", k.c_str()); return 2; }
        const std::string v = argv[++i];
        if (k == "--channel") channel = v;
        else if (k == "--stream-id") stream_id = std::strtol(v.c_str(), nullptr, 10);
        else if (k == "--aeron-dir") dir = v;
        else if (k == "--count") count = std::strtoull(v.c_str(), nullptr, 10);
        else if (k == "--rate") rate = std::strtoull(v.c_str(), nullptr, 10);
        else if (k == "--symbols") symbols = std::strtoull(v.c_str(), nullptr, 10);
        else if (k == "--heartbeat-every") hb_every = std::strtoull(v.c_str(), nullptr, 10);
        else if (k == "--driver-timeout-ms") driver_timeout_ms = std::strtoull(v.c_str(), nullptr, 10);
        else { std::fprintf(stderr, "sp_aeron_pub: unknown option %s\n", k.c_str()); return 2; }
    }
    if (symbols == 0) { std::fputs("sp_aeron_pub: --symbols must be > 0\n", stderr); return 2; }

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
    aeron_context_set_client_name(ctx, "sp_aeron_pub");
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
        if (std::chrono::steady_clock::now() > deadline) { std::fputs("sp_aeron_pub: no subscriber connected\n", stderr); return 1; }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    const uint64_t offer_timeout_ns = 5'000'000'000ull;
    const auto interval = std::chrono::nanoseconds(rate ? 1'000'000'000ull / rate : 0);
    auto next = std::chrono::steady_clock::now();
    uint64_t seq = 0, sent = 0, heartbeats = 0;
    while (count == 0 || sent < count) {
        const uint32_t sym = static_cast<uint32_t>(1 + sent % symbols);
        const int64_t px = 100'000'000'000ll + static_cast<int64_t>(sym) * 1'000'000'000ll + static_cast<int64_t>(sent % 50) * 10'000'000ll;
        bool ok;
        if (sent % 2 == 0) {
            Trade t; begin(t, sym, ++seq);
            t.Price = px; t.Qty = 250'000'000; t.TradeId = seq; t.Aggressor = static_cast<uint8_t>(1 + seq % 2);
            ok = offer(pub, &t, sizeof t, offer_timeout_ns);
        } else {
            Quote q; begin(q, sym, ++seq);
            q.BidPx = px - 10'000'000; q.BidQty = 1'000'000'000; q.AskPx = px + 10'000'000; q.AskQty = 2'000'000'000;
            ok = offer(pub, &q, sizeof q, offer_timeout_ns);
        }
        if (!ok) { std::fprintf(stderr, "sp_aeron_pub: offer failed after %llu messages\n", static_cast<unsigned long long>(sent)); return 1; }
        ++sent;
        if (hb_every && sent % hb_every == 0) {
            Heartbeat h; begin(h, 0, ++seq);
            if (!offer(pub, &h, sizeof h, offer_timeout_ns)) { std::fputs("sp_aeron_pub: heartbeat offer failed\n", stderr); return 1; }
            ++heartbeats;
        }
        if (interval.count()) { next += interval; std::this_thread::sleep_until(next); }
    }
    std::printf("sent %llu messages and %llu heartbeats on %s stream %ld\n",
                static_cast<unsigned long long>(sent), static_cast<unsigned long long>(heartbeats), channel.c_str(), stream_id);
    std::fflush(stdout);
    // Let the driver's sender flush over UDP before the publication goes away.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    aeron_exclusive_publication_close(pub, nullptr, nullptr);
    aeron_close(client);
    aeron_context_close(ctx);
    if (driver) { aeron_driver_close(driver); aeron_driver_context_close(dctx); }
    return 0;
}
