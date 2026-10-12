#include <doctest.h>

#include "flashtick/core/seqlock.h"

#include <atomic>
#include <thread>

using namespace flashtick;

namespace {
struct Pair { uint64_t a, b, c; };
}

TEST_CASE("seqlock basic store/load") {
    Seqlock<Pair> s;
    Pair p{};
    CHECK(s.load(p));
    CHECK(p.a == 0);
    CHECK(s.version() == 0);
    s.store({1, 1, 1});
    CHECK(s.load(p));
    CHECK(p.a == 1);
    CHECK(s.version() == 2);
}

TEST_CASE("seqlock reader never sees torn value") {
    Seqlock<Pair> s;
    std::atomic<bool> stop{false};
    std::thread w([&] {
        for (uint64_t i = 1; !stop.load(std::memory_order_relaxed); ++i) s.store({i, i, i});
    });
    uint64_t ok = 0;
    for (int i = 0; i < 200000; ++i) {
        Pair p;
        if (s.load(p)) {
            REQUIRE(p.a == p.b);
            REQUIRE(p.b == p.c);
            ++ok;
        }
    }
    stop = true;
    w.join();
    CHECK(ok > 0);
}
