#include <doctest.h>

#include "subplane/metrics/prom_writer.h"

#include <sstream>
#include <string>
#include <vector>

using namespace subplane;

namespace {
std::vector<std::string> lines(const std::string& s) {
    std::vector<std::string> out;
    std::istringstream in(s);
    for (std::string l; std::getline(in, l);) out.push_back(l);
    return out;
}
} // namespace

TEST_CASE("prom writer: one HELP and TYPE per family, its samples together, common labels first, values escaped") {
    PromWriter w({{"service", "subplaned"}, {"instance", "box1"}});
    w.counter("subplane_partition_packets_total", "Packets", {{"partition", "a"}}, 5);
    w.gauge("subplane_partition_last_sequence", "Last", {{"partition", "a"}}, 42);
    w.counter("subplane_partition_packets_total", "Packets", {{"partition", "b\"x\\y\nz"}}, 7);
    w.gauge("subplane_uptime_seconds", "Up\nline", {}, 1.5);
    const auto l = lines(w.str());
    REQUIRE(l.size() == 10);
    CHECK(l[0] == "# HELP subplane_partition_packets_total Packets");
    CHECK(l[1] == "# TYPE subplane_partition_packets_total counter");
    CHECK(l[2] == "subplane_partition_packets_total{service=\"subplaned\",instance=\"box1\",partition=\"a\"} 5");
    CHECK(l[3] == "subplane_partition_packets_total{service=\"subplaned\",instance=\"box1\",partition=\"b\\\"x\\\\y\\nz\"} 7");
    CHECK(l[4] == "# HELP subplane_partition_last_sequence Last");
    CHECK(l[5] == "# TYPE subplane_partition_last_sequence gauge");
    CHECK(l[6] == "subplane_partition_last_sequence{service=\"subplaned\",instance=\"box1\",partition=\"a\"} 42");
    CHECK(l[7] == "# HELP subplane_uptime_seconds Up\\nline");
    CHECK(l[9] == "subplane_uptime_seconds{service=\"subplaned\",instance=\"box1\"} 1.5");
}

TEST_CASE("prom writer: no labels at all gives a bare name; large integers stay exact") {
    PromWriter w;
    w.gauge("g", "h", {}, 1234567890123.0);
    w.counter("c_total", "h", {}, 18446744073709551615ull);
    const auto l = lines(w.str());
    CHECK(l[2] == "g 1234567890123");
    CHECK(l[5] == "c_total 18446744073709551615");
}

TEST_CASE("prom writer: a LogHistogram becomes cumulative power-of-two buckets, +Inf and count from the buckets") {
    LogHistogram h;
    for (uint64_t v : {0ull, 1ull, 2ull, 3ull, 4ull, 7ull, 8ull, 1000ull, 1ull << 45}) h.record(v);
    PromWriter w;
    w.histogram_ns("lat_seconds", "Latency", {{"group", "g0"}}, h.snapshot());
    const auto l = lines(w.str());
    REQUIRE(l.size() == 2 + PromWriter::kMajors + 3);
    CHECK(l[1] == "# TYPE lat_seconds histogram");
    CHECK(l[2] == "lat_seconds_bucket{group=\"g0\",le=\"0.000000001\"} 2");   // 0, 1
    CHECK(l[3] == "lat_seconds_bucket{group=\"g0\",le=\"0.000000003\"} 4");   // + 2, 3
    CHECK(l[4] == "lat_seconds_bucket{group=\"g0\",le=\"0.000000007\"} 6");   // + 4, 7
    CHECK(l[5] == "lat_seconds_bucket{group=\"g0\",le=\"0.000000015\"} 7");  // + 8
    CHECK(l[11] == "lat_seconds_bucket{group=\"g0\",le=\"0.000001023\"} 8"); // + 1000
    // 2^45 is beyond the last finite edge (2^40 - 1): only in +Inf.
    CHECK(l[2 + PromWriter::kMajors - 1] == "lat_seconds_bucket{group=\"g0\",le=\"1099.511627775\"} 8");
    CHECK(l[2 + PromWriter::kMajors] == "lat_seconds_bucket{group=\"g0\",le=\"+Inf\"} 9");
    CHECK(l[2 + PromWriter::kMajors + 1] == "lat_seconds_sum{group=\"g0\"} " + PromWriter::seconds(h.snapshot().sum));
    CHECK(l[2 + PromWriter::kMajors + 2] == "lat_seconds_count{group=\"g0\"} 9");

    CHECK(PromWriter::seconds(0) == "0");
    CHECK(PromWriter::seconds(1'500'000'000) == "1.5");
    CHECK(PromWriter::seconds(2'000'000'000) == "2");

    // Monotonic everywhere.
    uint64_t prev = 0;
    for (unsigned i = 0; i <= PromWriter::kMajors; ++i) {
        const std::string& s = l[2 + i];
        const uint64_t v = std::stoull(s.substr(s.rfind(' ') + 1));
        CHECK(v >= prev);
        prev = v;
    }
}
