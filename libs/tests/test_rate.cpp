#include <doctest.h>

#include "gcra.h"
#include "subplane/lib/fake_store.h"
#include "subplane/lib/rate.h"

#include <stdexcept>

using namespace subplane;
using lib::Bucket;
using lib::FakeStore;

namespace {

constexpr uint64_t kNsPerUs = 1000;
constexpr uint64_t kUsPerMs = 1000;
constexpr uint64_t kUsPerS = 1'000'000;

// One clock for the store (µs) and the limiter (ns).
struct Clock {
    FakeStore& store;
    uint64_t us = kUsPerS;  // not 0: a zero time means "never" in the limiter
    Clock(FakeStore& s) : store(s) { store.set_now_us(us); }
    uint64_t ns() const { return us * kNsPerUs; }
    void advance_ms(uint64_t ms) {
        us += ms * kUsPerMs;
        store.set_now_us(us);
    }
};

rate::Config one(uint64_t rate, uint64_t burst, uint64_t lease) {
    rate::Config c;
    c.path = {Bucket{"site", rate, burst}};
    c.lease = lease;
    return c;
}

uint64_t drain(rate::Limiter& l) {
    uint64_t n = 0;
    while (l.try_acquire(1)) ++n;
    return n;
}

}  // namespace

TEST_CASE("gcra: an idle bucket holds its burst, and refills at its rate") {
    const double now = 1e6;
    CHECK(lib::gcra::available(0, now, 1000, 50) == 50);
    // 50 taken: empty; 10 ms later, 10 tokens at 1000/s.
    const double tat = lib::gcra::after_take(0, now, 1000, 50);
    CHECK(lib::gcra::available(tat, now, 1000, 50) == 0);
    CHECK(lib::gcra::available(tat, now + 10'000, 1000, 50) == 10);
    CHECK(lib::gcra::wait_us(tat, now, 1000, 50, 5) == 5000);
    // Never more than the burst, however long idle.
    CHECK(lib::gcra::available(tat, now + 100 * 1e6, 1000, 50) == 50);
}

TEST_CASE("rate: config checks") {
    CHECK_FALSE(one(1000, 100, 10).check().size());
    rate::Config c = one(1000, 100, 10);
    c.path.clear();
    CHECK(c.check().find("empty") != std::string::npos);
    c = one(0, 100, 10);
    CHECK(c.check().find("rate 0") != std::string::npos);
    c = one(1000, 100, 0);
    CHECK(c.check().find("lease") != std::string::npos);
    c = one(1000, 5, 10);
    c.min_grant = 8;
    CHECK(c.check().find("burst") != std::string::npos);
    CHECK_THROWS_AS(rate::Limiter{one(1000, 100, 0)}, std::invalid_argument);
}

TEST_CASE("rate: a lease is spent locally and refilled below the low-water mark") {
    FakeStore s;
    Clock clk(s);
    rate::Limiter l(one(1000, 1000, 100));
    CHECK_FALSE(l.try_acquire(1));  // nothing leased yet
    l.step(s, clk.ns());
    CHECK(l.available() == 100);
    CHECK(l.try_acquire(40));
    l.step(s, clk.ns());  // 60 left: above low water (50), no refill
    CHECK(l.available() == 60);
    CHECK(l.try_acquire(20));
    l.step(s, clk.ns());  // 40: refilled up to the lease
    CHECK(l.available() == 100);
    CHECK(l.stats().granted == 160);
    CHECK_FALSE(l.try_acquire(101));
    CHECK(l.stats().throttled == 2);
    l.give_back(5);
    CHECK(l.available() == 105);
}

TEST_CASE("rate: after the burst, grants follow the rate") {
    FakeStore s;
    Clock clk(s);
    rate::Limiter l(one(1000, 200, 100));
    uint64_t spent = 0;
    // 2 s in 1 ms steps: at most burst + rate * 2 s.
    for (int i = 0; i < 2000; ++i) {
        l.step(s, clk.ns());
        spent += drain(l);
        clk.advance_ms(1);
    }
    CHECK(spent >= 2000);
    CHECK(spent <= 200 + 2000);
}

TEST_CASE("rate: a refused refill waits as long as the store says") {
    FakeStore s;
    Clock clk(s);
    rate::Config c = one(1000, 100, 100);
    c.min_grant = 50;
    rate::Limiter l(c);
    l.step(s, clk.ns());
    CHECK(drain(l) == 100);
    l.step(s, clk.ns());  // empty bucket: refused, retry in 50 ms
    CHECK(l.stats().denied == 1);
    clk.advance_ms(30);
    l.step(s, clk.ns());  // still waiting: no call
    CHECK(l.stats().denied == 1);
    CHECK(l.available() == 0);
    clk.advance_ms(20);
    l.step(s, clk.ns());  // 50 ms: 50 tokens
    CHECK(l.available() == 50);
}

TEST_CASE("rate: the parent bucket bounds its children together") {
    FakeStore s;
    Clock clk(s);
    // Two topics of 1000/s each under an interface of 1000/s.
    auto cfg = [](const char* topic) {
        rate::Config c;
        c.path = {Bucket{"site/eth0", 1000, 100}, Bucket{std::string("site/eth0/") + topic, 1000, 100}};
        c.lease = 20;
        return c;
    };
    rate::Limiter a(cfg("a"));
    rate::Limiter b(cfg("b"));
    uint64_t sa = 0;
    uint64_t sb = 0;
    // Nothing is fair between siblings yet (borrowing comes later): whoever asks
    // first gets the token, so the two take turns asking first.
    for (int i = 0; i < 1000; ++i) {
        rate::Limiter& first = i % 2 == 0 ? a : b;
        rate::Limiter& second = i % 2 == 0 ? b : a;
        first.step(s, clk.ns());
        second.step(s, clk.ns());
        sa += drain(a);
        sb += drain(b);
        clk.advance_ms(1);
    }
    // 1 s: the interface allows 1000 + its burst, shared.
    CHECK(sa + sb <= 1000 + 100 + 2 * 20);
    CHECK(sa + sb >= 1000);
    CHECK(sa > 300);
    CHECK(sb > 300);
}

TEST_CASE("rate: a grant is what the emptiest bucket allows") {
    FakeStore s;
    Clock clk(s);
    const Bucket parent{"p", 1000, 30};
    const Bucket child{"p/c", 1000, 1000};
    const Bucket path[] = {parent, child};
    const lib::Take t = s.take(path, 100, 1);
    CHECK(t.status == lib::Take::OK);
    CHECK(t.granted == 30);
    const lib::Take u = s.take(path, 100, 10);
    CHECK(u.granted == 0);
    CHECK(u.wait_us == 10 * kUsPerMs);
    // The refused take charged nothing.
    clk.advance_ms(10);
    CHECK(s.take(path, 100, 10).granted == 10);
}

TEST_CASE("rate: an unreachable store falls back to the floor rate, never more") {
    FakeStore s;
    Clock clk(s);
    rate::Config c = one(1'000'000, 1'000'000, 100);
    c.floor_rate = 1000;
    c.retry_ms = 100;
    rate::Limiter l(c);
    l.step(s, clk.ns());
    CHECK(drain(l) == 100);
    s.set_down(true);
    l.step(s, clk.ns());
    CHECK(l.stats().floor);
    CHECK(l.stats().store_errors == 1);
    uint64_t spent = 0;
    for (int i = 0; i < 500; ++i) {
        clk.advance_ms(1);
        l.step(s, clk.ns());
        spent += drain(l);
    }
    // 0.5 s at 1000/s, though the store would allow far more.
    CHECK(spent >= 490);
    CHECK(spent <= 500);
    CHECK(l.stats().store_errors == 1 + 5);  // then one try per retry_ms
    s.set_down(false);
    drain(l);
    clk.advance_ms(100);
    l.step(s, clk.ns());
    CHECK_FALSE(l.stats().floor);
    CHECK(l.available() >= 50);
}

TEST_CASE("rate: a full lease does not bank floor tokens") {
    FakeStore s;
    Clock clk(s);
    rate::Config c = one(1000, 1000, 100);
    c.floor_rate = 1000;
    rate::Limiter l(c);
    s.set_down(true);
    l.step(s, clk.ns());  // floor mode, lease empty
    clk.advance_ms(10'000);
    l.step(s, clk.ns());
    CHECK(l.available() == 100);
    CHECK(drain(l) == 100);
    clk.advance_ms(10);
    l.step(s, clk.ns());
    CHECK(l.available() == 10);  // not 10 s worth
}
