#include <doctest.h>

#include "fake_transport.h"
#include "subplane/core/clock.h"
#include "subplane/pipeline/partition.h"
#include "subplane/recap/aeron_recap.h"
#include "subplane/recap/recap_manager.h"
#include "subplane/symbol_table.h"

#include <aeronc.h>
#include <aeronmd.h>

#include <atomic>
#include <cstring>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace subplane;
using subplane::test::FakeTransport;

namespace {

std::string data_dir() {
    std::string f = __FILE__;
    return f.substr(0, f.find_last_of('/')) + "/../data";
}

void put(std::vector<uint8_t>& b, uint64_t v, unsigned size) {
    for (unsigned i = 0; i < size; ++i) b.push_back(static_cast<uint8_t>(v >> (8 * i)));
}
void header(std::vector<uint8_t>& b, uint16_t blk, uint16_t tid) { put(b, blk, 2); put(b, tid, 2); put(b, 7, 2); put(b, 2, 2); }

// A minimal snapshot service: answers each request with RECAP_BEGIN(as_of =
// last_seq_seen + 1), one trade for the requested instrument, RECAP_END, on an
// Aeron response publication matched to the requester's image.
struct SnapServer {
    aeron_context_t* ctx = nullptr;
    aeron_t* client = nullptr;
    aeron_subscription_t* sub = nullptr;
    aeron_exclusive_publication_t* resp = nullptr;
    std::atomic<int64_t> image_correlation{0};
    std::atomic<bool> stop{false};
    std::atomic<int> requests{0};
    std::string response_control;
    std::thread th;
    std::vector<RecapRequestWire> pending;

    static void on_image(void* clientd, aeron_subscription_t*, aeron_image_t* image) {
        aeron_image_constants_t c{};
        if (aeron_image_constants(image, &c) == 0) static_cast<SnapServer*>(clientd)->image_correlation.store(c.correlation_id);
    }
    static void on_fragment(void* clientd, const uint8_t* buf, size_t len, aeron_header_t*) {
        auto* self = static_cast<SnapServer*>(clientd);
        if (len != sizeof(RecapRequestWire)) return;
        RecapRequestWire w;
        std::memcpy(&w, buf, sizeof(w));
        if (w.magic != kRecapRequestMagic) return;
        self->pending.push_back(w);
        ++self->requests;
    }
    bool offer(const std::vector<uint8_t>& b) {
        for (int i = 0; i < 100000; ++i) {
            if (aeron_exclusive_publication_offer(resp, b.data(), b.size(), nullptr, nullptr) >= 0) return true;
            sched_yield();
        }
        return false;
    }
    void answer(const RecapRequestWire& w) {
        std::vector<uint8_t> begin; header(begin, 16, 101); put(begin, w.request_id, 8); put(begin, w.last_seq_seen + 1, 8);
        std::vector<uint8_t> trade; header(trade, 39, 1); put(trade, w.instrument, 8); put(trade, 1, 8); put(trade, 0, 8); put(trade, 0, 2);
        trade.insert(trade.end(), 39 - 26, 0);
        std::vector<uint8_t> end; header(end, 8, 100); put(end, w.request_id, 8);
        offer(begin);
        offer(trade);
        offer(end);
    }
    bool start(const std::string& dir, const std::string& request_channel, const std::string& control, std::string& err) {
        response_control = control;
        if (aeron_context_init(&ctx) < 0 || aeron_context_set_dir(ctx, dir.c_str()) < 0 || aeron_init(&client, ctx) < 0 || aeron_start(client) < 0) {
            err = aeron_errmsg(); return false;
        }
        aeron_async_add_subscription_t* async = nullptr;
        if (aeron_async_add_subscription(&async, client, request_channel.c_str(), 1101, &SnapServer::on_image, this, nullptr, nullptr) < 0) { err = aeron_errmsg(); return false; }
        while (aeron_async_add_subscription_poll(&sub, async) == 0) sched_yield();
        if (!sub) { err = aeron_errmsg(); return false; }
        th = std::thread([this] { run(); });
        return true;
    }
    void run() {
        aeron_fragment_assembler_t* fa = nullptr;
        aeron_fragment_assembler_create(&fa, &SnapServer::on_fragment, this);
        while (!stop.load()) {
            aeron_subscription_poll(sub, aeron_fragment_assembler_handler, fa, 10);
            if (!resp && image_correlation.load() != 0) {
                const std::string ch = "aeron:udp?control-mode=response|control=" + response_control + "|term-length=65536" +
                                       "|response-correlation-id=" + std::to_string(image_correlation.load());
                aeron_async_add_exclusive_publication_t* ap = nullptr;
                if (aeron_async_add_exclusive_publication(&ap, client, ch.c_str(), 1102) == 0) {
                    int rc;
                    while ((rc = aeron_async_add_exclusive_publication_poll(&resp, ap)) == 0) sched_yield();
                    if (rc < 0) { MESSAGE("snap server response publication: " << aeron_errmsg()); resp = nullptr; image_correlation.store(0); }
                }
            }
            if (resp && aeron_exclusive_publication_is_connected(resp)) {
                for (const auto& w : pending) answer(w);
                pending.clear();
            }
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
        aeron_fragment_assembler_delete(fa);
    }
    ~SnapServer() {
        stop.store(true);
        if (th.joinable()) th.join();
        if (resp) aeron_exclusive_publication_close(resp, nullptr, nullptr);
        if (sub) aeron_subscription_close(sub, nullptr, nullptr);
        if (client) aeron_close(client);
        if (ctx) aeron_context_close(ctx);
    }
};

struct Driver {
    aeron_driver_context_t* ctx = nullptr;
    aeron_driver_t* drv = nullptr;
    bool start(const std::string& dir, std::string& err) {
        if (aeron_driver_context_init(&ctx) < 0) { err = aeron_errmsg(); return false; }
        aeron_driver_context_set_dir(ctx, dir.c_str());
        aeron_driver_context_set_threading_mode(ctx, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(ctx, true);
        aeron_driver_context_set_dir_delete_on_shutdown(ctx, true);
        if (aeron_driver_init(&drv, ctx) < 0 || aeron_driver_start(drv, false) < 0) { err = aeron_errmsg(); return false; }
        return true;
    }
    ~Driver() {
        if (drv) aeron_driver_close(drv);
        if (ctx) aeron_driver_context_close(ctx);
    }
};

} // namespace

// INTENT: small term buffers. Two publications at Aeron's 16 MB default need
// about 100 MB of /dev/shm, more than a default Docker container has (64 MB);
// CI runs in one.
TEST_CASE("recap over Aeron: request out, reply back on a response channel, assembled by the manager") {
    const std::string dir = "/dev/shm/subplane-recaptest-" + std::to_string(getpid());
    const int base = 21000 + static_cast<int>(getpid() % 20000);
    const std::string req_ep = "127.0.0.1:" + std::to_string(base);
    const std::string ctl_ep = "127.0.0.1:" + std::to_string(base + 1);
    std::string err;
    Driver drv;
    REQUIRE_MESSAGE(drv.start(dir, err), err);
    SnapServer srv;
    REQUIRE_MESSAGE(srv.start(dir, "aeron:udp?endpoint=" + req_ep, ctl_ep, err), err);

    const std::string yaml = R"(
name: p0
transport: { type: udp_multicast, group: "", port: 1 }
framing: { sequence: message.SeqNum }
decoder:
  type: sbe_routing
  schema: sample_md.xml
  fields: { symbol: SecurityID, sequence: SeqNum }
  messages:
    - { template: 1,   msg_type: TRADE }
    - { template: 101, admin: RECAP_BEGIN, fields: { request_id: SeqNum, as_of: NewSeq } }
    - { template: 100, admin: RECAP_END,   fields: { request_id: SeqNum } }
recap:
  type: aeron
  aeron_dir: )" + dir + R"(
  request:  { channel: "aeron:udp?endpoint=)" + req_ep + R"(|term-length=65536", stream_id: 1101 }
  response: { channel: "aeron:udp?control-mode=response|control=)" + ctl_ep + R"(", stream_id: 1102 }
  timeout_ms: 500
  retry: { base_ms: 20, max_ms: 100 }
)";
    SymbolTable symbols(1024);
    PartitionConfig cfg;
    REQUIRE_MESSAGE(PartitionConfig::parse(YAML::Load(yaml), 0, data_dir(), cfg, err), err);
    Partition part;
    REQUIRE_MESSAGE(part.build(cfg, err, std::make_unique<FakeTransport>()), err);
    REQUIRE(part.has_recap());
    RecapChannel ch;
    RecapManager mgr(4242, symbols);
    REQUIRE_MESSAGE(mgr.add_partition(part, ch, err), err);
    REQUIRE_MESSAGE(mgr.open(err), err);
    part.last_seq_pub.store(900);
    mgr.start();

    // Until the service has its response publication up, requests time out and
    // are retried; the first full reply ends the wait.
    const uint32_t sym = symbols.intern_u64(31337);
    REQUIRE(ch.requests.push({RecapCommand::Op::REQUEST, 0, sym}));
    RecapReply ok{};
    bool got = false;
    const uint64_t deadline = now_mono_ns() + 10'000'000'000ull;
    while (!got && now_mono_ns() < deadline) {
        RecapReply r;
        while (ch.replies.pop(r)) {
            if (r.status == RecapStatus::OK) { ok = r; got = true; }
            else MESSAGE("recap reply status " << recap_status_name(r.status));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    mgr.stop();
    REQUIRE(got);
    CHECK(srv.requests.load() >= 1);
    CHECK(ok.symbol_id == sym);
    CHECK(ok.as_of == 901);
    REQUIRE(ok.messages == 1);
    uint32_t len;
    std::memcpy(&len, ok.data, 4);
    CHECK(len == 47);
    uint64_t sec;
    std::memcpy(&sec, ok.data + 4 + 8, 8);
    CHECK(sec == 31337);
    CHECK(part.recap_counters.recap_replies.load() == 1);
}
