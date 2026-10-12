#include <doctest.h>

#include "flashtick/core/histogram.h"

using namespace flashtick;

TEST_CASE("histogram bucket edges") {
    for (uint64_t v : std::initializer_list<uint64_t>{1ull, 2ull, 3ull, 4ull, 5ull, 7ull, 8ull, 100ull, 1000ull, 123456789ull, UINT64_MAX}) {
        const unsigned i = LogHistogram::bucket_index(v);
        CHECK(LogHistogram::bucket_lower_edge(i) <= v);
        CHECK(LogHistogram::bucket_upper_edge(i) >= v);
    }
    CHECK(LogHistogram::bucket_index(0) == 0);
    CHECK(LogHistogram::bucket_lower_edge(LogHistogram::bucket_index(1000)) == 896);
    CHECK(LogHistogram::bucket_upper_edge(LogHistogram::bucket_index(1000)) == 1023);
}

TEST_CASE("histogram percentiles") {
    LogHistogram h;
    CHECK(h.snapshot().min == 0);
    CHECK(percentile(h.snapshot(), 0.5) == 0);
    for (int i = 0; i < 99; ++i) h.record(1000);
    h.record(1'000'000);
    const auto s = h.snapshot();
    CHECK(s.count == 100);
    CHECK(s.min == 1000);
    CHECK(s.max == 1'000'000);
    CHECK(s.sum == 99 * 1000 + 1'000'000);
    CHECK(percentile(s, 0.5) == 1023);
    CHECK(percentile(s, 0.99) == 1023);
    CHECK(percentile(s, 1.0) == LogHistogram::bucket_upper_edge(LogHistogram::bucket_index(1'000'000)));
    CHECK(mean(s) == doctest::Approx(10990.0));
}

TEST_CASE("histogram json") {
    LogHistogram h;
    h.record(0);
    h.record(50);
    const std::string j = to_json(h.snapshot(), "decode_ns");
    CHECK(j.find("\"decode_ns\"") != std::string::npos);
    CHECK(j.find("\"count\":2") != std::string::npos);
    CHECK(j.front() == '{');
    CHECK(j.back() == '}');
}
