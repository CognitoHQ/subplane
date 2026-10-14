#include <doctest.h>

#include "subplane/symbol_table.h"

#include <string>
#include <thread>
#include <vector>

using namespace subplane;

TEST_CASE("symbol table intern/lookup/resolve") {
    SymbolTable t(100);
    CHECK(t.capacity() == 100);
    CHECK(t.lookup("AAPL") == 0);
    const uint32_t a = t.intern("AAPL");
    const uint32_t b = t.intern("MSFT");
    CHECK(a == 1);
    CHECK(b == 2);
    CHECK(t.intern("AAPL") == a);
    CHECK(t.lookup("MSFT") == b);
    CHECK(t.size() == 2);
    std::string s;
    CHECK(t.resolve(b, s));
    CHECK(s == "MSFT");
    CHECK_FALSE(t.resolve(0, s));
    CHECK_FALSE(t.resolve(3, s));
    CHECK(t.intern("") == 0);
    CHECK(t.intern(std::string(25, 'x')) == 0);
    const std::string max24(24, 'y');
    const uint32_t m = t.intern(max24);
    CHECK(m != 0);
    CHECK(t.resolve(m, s));
    CHECK(s == max24);
}

TEST_CASE("symbol table full returns 0") {
    SymbolTable t(3);
    CHECK(t.intern("A") != 0);
    CHECK(t.intern("B") != 0);
    CHECK(t.intern("C") != 0);
    CHECK(t.intern("D") == 0);
    CHECK(t.intern("D") == 0);
    CHECK(t.intern("A") == 1); // existing still resolves
    CHECK(t.intern_u64(42) == 0);
    CHECK(t.size() == 3);
}

TEST_CASE("symbol table u64 path") {
    SymbolTable t(10);
    const uint32_t a = t.intern_u64(1234567890123ull);
    const uint32_t s = t.intern("X");
    CHECK(a != 0);
    CHECK(a != s);
    CHECK(t.intern_u64(1234567890123ull) == a);
    CHECK(t.lookup_u64(1234567890123ull) == a);
    CHECK(t.lookup_u64(5) == 0);
    CHECK(t.key_u64(a) == 1234567890123ull);
    CHECK(t.key_u64(s) == 0);
    std::string out;
    CHECK(t.resolve(a, out));
    CHECK(out == "1234567890123");
}

TEST_CASE("symbol table concurrent intern") {
    constexpr int kThreads = 4, kNames = 2000;
    SymbolTable t(kNames);
    std::vector<std::vector<uint32_t>> ids(kThreads, std::vector<uint32_t>(kNames, 0));
    std::vector<std::thread> th;
    for (int k = 0; k < kThreads; ++k) {
        th.emplace_back([&, k] {
            for (int i = 0; i < kNames; ++i) {
                // each thread walks the overlapping set in a different order
                const int mult[kThreads] = {1, 3, 7, 9}; // coprime to kNames => permutation
                const int n = (i * mult[k] + k * 17) % kNames;
                ids[static_cast<std::size_t>(k)][static_cast<std::size_t>(n)] =
                    t.intern("SYM" + std::to_string(n));
            }
        });
    }
    for (auto& x : th) x.join();
    CHECK(t.size() == kNames);
    std::vector<bool> seen(kNames + 1, false);
    for (int n = 0; n < kNames; ++n) {
        const uint32_t id = ids[0][static_cast<std::size_t>(n)];
        REQUIRE(id != 0);
        CHECK_FALSE(seen[id]);
        seen[id] = true;
        for (int k = 1; k < kThreads; ++k) REQUIRE(ids[static_cast<std::size_t>(k)][static_cast<std::size_t>(n)] == id);
        std::string s;
        REQUIRE(t.resolve(id, s));
        REQUIRE(s == "SYM" + std::to_string(n));
    }
}

TEST_CASE("venue table") {
    VenueTable v;
    const VenueId a = v.intern("XNAS");
    CHECK(a == 1);
    CHECK(v.intern("XNAS") == a);
    CHECK(v.lookup("XNYS") == 0);
    std::string s;
    CHECK(v.resolve(a, s));
    CHECK(s == "XNAS");
}

TEST_CASE("symbol table: venue-scoped integer ids") {
    SymbolTable t(16);
    const uint32_t g = t.intern_u64(1);
    const uint32_t a = t.intern_u64(1, 10);
    const uint32_t b = t.intern_u64(1, 20);
    CHECK(g != 0);
    CHECK(a != 0);
    CHECK(b != 0);
    CHECK(g != a);
    CHECK(a != b);
    CHECK(t.intern_u64(1, 10) == a);
    CHECK(t.lookup_u64(1, 20) == b);
    CHECK(t.lookup_u64(1) == g);
    CHECK(t.lookup_u64(1, 30) == 0);
    CHECK(t.venue_u64(a) == 10);
    CHECK(t.venue_u64(g) == 0);
    CHECK(t.key_u64(b) == 1);
    std::string out;
    REQUIRE(t.resolve(a, out));
    CHECK(out == "10:1");
    REQUIRE(t.resolve(g, out));
    CHECK(out == "1");
}

TEST_CASE("parse_numeric_symbol") {
    uint64_t k = 0;
    uint16_t v = 0;
    REQUIRE(parse_numeric_symbol("1001", k, v));
    CHECK(k == 1001);
    CHECK(v == 0);
    REQUIRE(parse_numeric_symbol("10:7", k, v));
    CHECK(k == 7);
    CHECK(v == 10);
    CHECK_FALSE(parse_numeric_symbol("", k, v));
    CHECK_FALSE(parse_numeric_symbol("ABC", k, v));
    CHECK_FALSE(parse_numeric_symbol("0:7", k, v));      // venue 0 is the global scope
    CHECK_FALSE(parse_numeric_symbol("65536:7", k, v));  // venue is u16
    CHECK_FALSE(parse_numeric_symbol("10:", k, v));
    CHECK_FALSE(parse_numeric_symbol(":7", k, v));
    CHECK_FALSE(parse_numeric_symbol("10:7:1", k, v));
    CHECK_FALSE(parse_numeric_symbol("BTC:1", k, v));
}
