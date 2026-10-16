#pragma once

// Prometheus text exposition (format 0.0.4) writer, cold path only
// (docs/context/observability.md). Samples may be added in any order; str()
// emits each metric family once, with its HELP and TYPE lines, and all of its
// samples together, as the format requires.

#include "subplane/core/histogram.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace subplane {

class PromWriter {
public:
    using Labels = std::vector<std::pair<std::string, std::string>>;

    // `common` labels go first on every sample (service, instance, host).
    explicit PromWriter(Labels common = {}) : common_(std::move(common)) {}

    // Counter names end in _total; the caller passes the full name.
    void counter(std::string_view name, std::string_view help, const Labels& labels, uint64_t value);
    void gauge(std::string_view name, std::string_view help, const Labels& labels, double value);
    // A nanosecond LogHistogram as a Prometheus histogram in seconds (the
    // base unit; name it `..._seconds`). Buckets are its power-of-two majors:
    // le = (2^(m+1) - 1) ns for m = 0..kMajors-1 (1 ns to about 18 minutes),
    // then +Inf. The same edges on every scrape, so rate() across scrapes works.
    void histogram_ns(std::string_view name, std::string_view help, const Labels& labels, const LogHistogram::Snapshot& s);
    static std::string seconds(uint64_t ns);  // exact decimal of ns / 1e9

    std::string str() const;

    static constexpr unsigned kMajors = 40;
    static std::string escape(std::string_view v); // label value: \\, \" and \n

private:
    struct Family {
        std::string name, help, type;
        std::string body;
    };
    Family& family(std::string_view name, std::string_view help, const char* type);
    void label_set(std::string& out, const Labels& labels, std::string_view extra_key = {}, std::string_view extra_value = {}) const;

    Labels common_;
    std::vector<Family> families_;
    std::unordered_map<std::string, size_t> index_;
};

} // namespace subplane
