#include "subplane/metrics/prom_writer.h"

#include <cmath>
#include <cstdio>

namespace subplane {

std::string PromWriter::escape(std::string_view v) {
    std::string o;
    o.reserve(v.size());
    for (char c : v) {
        if (c == '\\') o += "\\\\";
        else if (c == '"') o += "\\\"";
        else if (c == '\n') o += "\\n";
        else o += c;
    }
    return o;
}

PromWriter::Family& PromWriter::family(std::string_view name, std::string_view help, const char* type) {
    const std::string key(name);
    if (auto it = index_.find(key); it != index_.end()) return families_[it->second];
    index_.emplace(key, families_.size());
    families_.push_back({key, std::string(help), type, {}});
    return families_.back();
}

void PromWriter::label_set(std::string& out, const Labels& labels, std::string_view extra_key, std::string_view extra_value) const {
    bool any = false;
    auto put = [&](std::string_view k, std::string_view v) {
        out += any ? ',' : '{';
        any = true;
        out.append(k);
        out += "=\"";
        out += escape(v);
        out += '"';
    };
    for (const auto& [k, v] : common_) put(k, v);
    for (const auto& [k, v] : labels) put(k, v);
    if (!extra_key.empty()) put(extra_key, extra_value);
    if (any) out += '}';
}

void PromWriter::counter(std::string_view name, std::string_view help, const Labels& labels, uint64_t value) {
    Family& f = family(name, help, "counter");
    f.body.append(name);
    label_set(f.body, labels);
    f.body += ' ';
    f.body += std::to_string(value);
    f.body += '\n';
}

void PromWriter::gauge(std::string_view name, std::string_view help, const Labels& labels, double value) {
    Family& f = family(name, help, "gauge");
    f.body.append(name);
    label_set(f.body, labels);
    char buf[64];
    if (std::isnan(value)) std::snprintf(buf, sizeof buf, " NaN\n");
    else if (value == std::floor(value) && std::fabs(value) < 9.0e15) std::snprintf(buf, sizeof buf, " %.0f\n", value);
    else std::snprintf(buf, sizeof buf, " %.9g\n", value);
    f.body += buf;
}

std::string PromWriter::seconds(uint64_t ns) {
    // INTENT: exact, no floating point: an le label must be the same string
    // on every scrape, and 2^k - 1 ns has no short exact binary form.
    std::string frac = std::to_string(ns % 1'000'000'000ull);
    frac.insert(0, 9 - frac.size(), '0');
    while (!frac.empty() && frac.back() == '0') frac.pop_back();
    std::string o = std::to_string(ns / 1'000'000'000ull);
    if (!frac.empty()) o += '.' + frac;
    return o;
}

void PromWriter::histogram_ns(std::string_view name, std::string_view help, const Labels& labels, const LogHistogram::Snapshot& s) {
    Family& f = family(name, help, "histogram");
    const std::string bucket = std::string(name) + "_bucket";
    uint64_t cum = 0;
    unsigned i = 0;
    for (unsigned m = 0; m < kMajors; ++m) {
        // INTENT: LogHistogram buckets 4m..4m+3 hold the values of major m,
        // whose inclusive upper edge is 2^(m+1) - 1. Summing them gives exact
        // cumulative counts at these edges; the sub-bucket detail is dropped
        // to keep about 40 series per histogram instead of 256.
        for (; i < 4 * (m + 1); ++i) cum += s.buckets[i];
        f.body += bucket;
        label_set(f.body, labels, "le", seconds((uint64_t{2} << m) - 1));
        f.body += ' ' + std::to_string(cum) + '\n';
    }
    // INTENT: +Inf and _count are the sum of the buckets, not s.count. The
    // snapshot is not atomic across fields (the writer keeps recording), and
    // a +Inf below the last finite bucket is an invalid histogram.
    for (; i < LogHistogram::kBuckets; ++i) cum += s.buckets[i];
    f.body += bucket;
    label_set(f.body, labels, "le", "+Inf");
    f.body += ' ' + std::to_string(cum) + '\n';
    f.body.append(name);
    f.body += "_sum";
    label_set(f.body, labels);
    f.body += ' ' + seconds(s.sum) + '\n';
    f.body.append(name);
    f.body += "_count";
    label_set(f.body, labels);
    f.body += ' ' + std::to_string(cum) + '\n';
}

std::string PromWriter::str() const {
    std::string o;
    for (const auto& f : families_) {
        o += "# HELP " + f.name + ' ';
        for (char c : f.help) {
            if (c == '\\') o += "\\\\";
            else if (c == '\n') o += "\\n";
            else o += c;
        }
        o += "\n# TYPE " + f.name + ' ' + f.type + '\n';
        o += f.body;
    }
    return o;
}

} // namespace subplane
