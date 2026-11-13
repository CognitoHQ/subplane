// ValkeyStore against a real valkey-server: SUBPLANE_VALKEY_PORT names it
// (valkey_e2e.sh starts one). The same rules as the FakeStore tests, through
// the Lua scripts.
#include <doctest.h>

#include "subplane/lib/valkey_store.h"

#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>
#include <unistd.h>

using namespace subplane::lib;

namespace {

int port() {
    const char* p = std::getenv("SUBPLANE_VALKEY_PORT");
    REQUIRE_MESSAGE(p != nullptr, "SUBPLANE_VALKEY_PORT is not set (run valkey_e2e.sh)");
    return std::atoi(p);
}

// A key prefix of its own per test, so tests never see each other's state.
ValkeyStore store(const char* test) {
    ValkeyOptions o;
    o.port = port();
    o.prefix = "{t" + std::to_string(getpid()) + "." + test + "}:";
    return ValkeyStore(o);
}

void sleep_ms(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

}  // namespace

TEST_CASE("valkey: a take is what the emptiest bucket allows, and a refusal charges nothing") {
    ValkeyStore s = store("take");
    // 10 tokens/s: the few ms between calls add nothing.
    const Bucket path[] = {Bucket{"p", 10, 30}, Bucket{"p/c", 10, 1000}};
    const Take t = s.take(path, 100, 1);
    REQUIRE_MESSAGE(t.status == Take::OK, s.last_error());
    CHECK(t.granted == 30);
    const Take u = s.take(path, 100, 5);
    CHECK(u.status == Take::OK);
    CHECK(u.granted == 0);
    CHECK(u.wait_us > 400'000);   // 5 tokens at 10/s: about 500 ms
    CHECK(u.wait_us <= 500'000);
    sleep_ms(520);
    CHECK(s.take(path, 100, 5).granted == 5);
    // The child was charged only for what was granted (35 of 1000), and has
    // refilled about 5 at 10/s during the sleep.
    const Bucket child[] = {Bucket{"p/c", 10, 1000}};
    const uint64_t left = s.take(child, 1000, 1).granted;
    CHECK(left >= 965);
    CHECK(left <= 971);
}

TEST_CASE("valkey: claim, renew, release, and the epoch") {
    ValkeyStore s = store("elect");
    const Claim a = s.claim("g", "a", 5000);
    REQUIRE_MESSAGE(a.status == Claim::HELD, s.last_error());
    CHECK(a.epoch == 1);
    const Claim b = s.claim("g", "b", 5000);
    CHECK(b.status == Claim::TAKEN);
    CHECK(b.epoch == 1);
    CHECK(s.claim("g", "a", 5000).status == Claim::HELD);  // the holder keeps it, same epoch
    CHECK(s.renew("g", "a", 1, 5000) == Renew::OK);
    CHECK(s.renew("g", "b", 1, 5000) == Renew::LOST);
    CHECK(s.renew("g", "a", 2, 5000) == Renew::LOST);  // a stale epoch
    s.release("g", "b", 1);                             // not the holder: no-op
    CHECK(s.claim("g", "b", 5000).status == Claim::TAKEN);
    s.release("g", "a", 1);
    const Claim c = s.claim("g", "b", 5000);
    CHECK(c.status == Claim::HELD);
    CHECK(c.epoch == 2);
}

TEST_CASE("valkey: a lease that is not renewed lapses") {
    ValkeyStore s = store("lapse");
    REQUIRE(s.claim("g", "a", 200).status == Claim::HELD);
    CHECK(s.claim("g", "b", 200).status == Claim::TAKEN);
    sleep_ms(300);
    CHECK(s.renew("g", "a", 1, 200) == Renew::LOST);
    const Claim b = s.claim("g", "b", 200);
    CHECK(b.status == Claim::HELD);
    CHECK(b.epoch == 2);
}

TEST_CASE("valkey: scripts are loaded again after the server drops them") {
    ValkeyStore s = store("flush");
    REQUIRE(s.claim("g", "a", 5000).status == Claim::HELD);
    const std::string cmd = "valkey-cli -p " + std::to_string(port()) + " SCRIPT FLUSH >/dev/null";
    REQUIRE(std::system(cmd.c_str()) == 0);
    CHECK(s.renew("g", "a", 1, 5000) == Renew::OK);
}

TEST_CASE("valkey: an unreachable server is an error, not a wait") {
    ValkeyOptions o;
    o.port = 1;  // nothing listens there
    ValkeyStore s(o);
    const auto t0 = std::chrono::steady_clock::now();
    const Bucket path[] = {Bucket{"p", 10, 30}};
    CHECK(s.take(path, 1, 1).status == Take::ERROR);
    CHECK(s.claim("g", "a", 1000).status == Claim::ERROR);  // within reconnect_ms: no new attempt
    CHECK(s.renew("g", "a", 1, 1000) == Renew::ERROR);
    CHECK_FALSE(s.last_error().empty());
    CHECK(std::chrono::steady_clock::now() - t0 < std::chrono::milliseconds(o.connect_timeout_ms * 2));
}
