#include <doctest.h>

#include "subplane/lib/elect.h"
#include "subplane/lib/fake_store.h"

#include <stdexcept>

using namespace subplane;
using elect::Candidate;
using elect::Change;
using lib::FakeStore;

namespace {

constexpr uint64_t kNsPerUs = 1000;
constexpr uint64_t kUsPerMs = 1000;
constexpr uint32_t kTtlMs = 3000;

struct Clock {
    FakeStore& store;
    uint64_t us = 1'000'000;
    Clock(FakeStore& s) : store(s) { store.set_now_us(us); }
    uint64_t ns() const { return us * kNsPerUs; }
    void advance_ms(uint64_t ms) {
        us += ms * kUsPerMs;
        store.set_now_us(us);
    }
};

elect::Config cfg(const char* id) {
    elect::Config c;
    c.group = "gw.binance";
    c.id = id;
    c.ttl_ms = kTtlMs;
    return c;
}

}  // namespace

TEST_CASE("elect: config checks and process ids") {
    CHECK(cfg("a").check().empty());
    elect::Config c = cfg("a");
    c.group.clear();
    CHECK_FALSE(c.check().empty());
    c = cfg("a");
    c.margin_ms = kTtlMs / 3;
    CHECK_FALSE(c.check().empty());
    CHECK_THROWS_AS(Candidate{c}, std::invalid_argument);
    // A restarted process is a new candidate.
    CHECK(Candidate::process_id() != Candidate::process_id());
    elect::Config d = cfg("");
    const Candidate anon(d);
    CHECK_FALSE(anon.config().id.empty());
}

TEST_CASE("elect: the first claim wins; the other copy stays secondary") {
    FakeStore s;
    Clock clk(s);
    Candidate a(cfg("a"));
    Candidate b(cfg("b"));
    CHECK(a.step(s, clk.ns()) == Change::ELECTED);
    CHECK(a.epoch() == 1);
    CHECK(a.is_primary(clk.ns()));
    CHECK(b.step(s, clk.ns()) == Change::NONE);
    CHECK_FALSE(b.is_primary(clk.ns()));
    // A renewing primary keeps the group for as long as it runs.
    for (int i = 0; i < 100; ++i) {
        clk.advance_ms(100);
        CHECK(a.step(s, clk.ns()) == Change::NONE);
        CHECK(b.step(s, clk.ns()) == Change::NONE);
    }
    CHECK(a.is_primary(clk.ns()));
    CHECK(a.stats().renews > 0);
    CHECK(b.stats().claims > 0);
}

TEST_CASE("elect: a stalled primary stops being primary before the store hands the group on") {
    FakeStore s;
    Clock clk(s);
    Candidate a(cfg("a"));
    Candidate b(cfg("b"));
    const uint64_t t0 = clk.us;
    CHECK(a.step(s, clk.ns()) == Change::ELECTED);
    // a stops stepping; b keeps trying every 10 ms.
    uint64_t a_until_us = 0;
    uint64_t b_from_us = 0;
    while (b_from_us == 0) {
        clk.advance_ms(10);
        if (a.is_primary(clk.ns())) a_until_us = clk.us;
        if (b.step(s, clk.ns()) == Change::ELECTED) b_from_us = clk.us;
    }
    // By its own clock a is primary for ttl - margin; the store holds the group
    // for it until ttl; b takes over within one claim interval after that.
    CHECK(a_until_us - t0 < (kTtlMs - kTtlMs / 10) * kUsPerMs);
    CHECK(b_from_us - t0 >= kTtlMs * kUsPerMs);
    CHECK(b_from_us - t0 <= (kTtlMs + kTtlMs / 3) * kUsPerMs);
    CHECK(b.epoch() == 2);
    // a wakes up: its deadline has passed.
    CHECK(a.step(s, clk.ns()) == Change::EXPIRED);
    CHECK(a.stats().transitions == 2);
}

TEST_CASE("elect: an unreachable store: the primary keeps trying, then steps down in time") {
    FakeStore s;
    Clock clk(s);
    Candidate a(cfg("a"));
    CHECK(a.step(s, clk.ns()) == Change::ELECTED);
    s.set_down(true);
    Change last = Change::NONE;
    uint64_t steps = 0;
    while (last == Change::NONE) {
        clk.advance_ms(10);
        ++steps;
        last = a.step(s, clk.ns());
    }
    CHECK(last == Change::EXPIRED);
    CHECK(steps * 10 <= kTtlMs - kTtlMs / 10);
    CHECK(a.stats().store_errors > 1);
    // Back up: a claims again, the lease has lapsed, so a new epoch.
    s.set_down(false);
    clk.advance_ms(kTtlMs);
    CHECK(a.step(s, clk.ns()) == Change::ELECTED);
    CHECK(a.epoch() == 2);
}

TEST_CASE("elect: a renewal that finds another holder is LOST") {
    FakeStore s;
    Clock clk(s);
    Candidate a(cfg("a"));
    Candidate b(cfg("b"));
    CHECK(a.step(s, clk.ns()) == Change::ELECTED);
    s.expire("gw.binance");  // the lease lapsed in the store (e.g. a failover lost it)
    CHECK(b.step(s, clk.ns()) == Change::ELECTED);
    clk.advance_ms(kTtlMs / 3);
    CHECK(a.step(s, clk.ns()) == Change::LOST);
    CHECK_FALSE(a.is_primary(clk.ns()));
    CHECK(b.epoch() > 1);
}

TEST_CASE("elect: stop gives the group up at once, with a higher epoch next") {
    FakeStore s;
    Clock clk(s);
    Candidate a(cfg("a"));
    Candidate b(cfg("b"));
    CHECK(a.step(s, clk.ns()) == Change::ELECTED);
    CHECK(b.step(s, clk.ns()) == Change::NONE);
    CHECK(a.stop(s) == Change::STOPPED);
    CHECK(a.stop(s) == Change::NONE);
    clk.advance_ms(kTtlMs / 3);
    CHECK(b.step(s, clk.ns()) == Change::ELECTED);
    CHECK(b.epoch() == 2);
}

TEST_CASE("elect: groups are independent") {
    FakeStore s;
    Clock clk(s);
    elect::Config x = cfg("a");
    elect::Config y = cfg("a");
    y.group = "snap.binance";
    Candidate a(x);
    Candidate c(y);
    CHECK(a.step(s, clk.ns()) == Change::ELECTED);
    CHECK(c.step(s, clk.ns()) == Change::ELECTED);
    CHECK(a.epoch() == 1);
    CHECK(c.epoch() == 1);
}
