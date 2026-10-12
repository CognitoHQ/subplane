#pragma once

// Shared helpers for the operator tools (cold path, clarity over speed):
// a tiny CLI parser, framing-block loading (file or inline YAML) and schema +
// length-walker loading.

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <ctime>
#include <thread>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <set>
#include <string>

#include <yaml-cpp/yaml.h>

#include "flashtick/decoder/sbe_schema.h"
#include "flashtick/framing/framing.h"
#include "flashtick/framing/sbe_length.h"

namespace fttool {

// `--key value`, `--key=value` and bare `--flag`. Unknown options are errors.
class Args {
public:
    bool parse(int argc, char** argv, const std::set<std::string>& valued,
               const std::set<std::string>& flags, std::string& err) {
        for (int i = 1; i < argc; ++i) {
            std::string a = argv[i];
            if (a.rfind("--", 0) != 0) { err = "unexpected argument: " + a; return false; }
            std::string key = a.substr(2);
            std::string val;
            bool has_val = false;
            const size_t eq = key.find('=');
            if (eq != std::string::npos) { val = key.substr(eq + 1); key = key.substr(0, eq); has_val = true; }
            if (flags.count(key)) { vals_[key] = "1"; continue; }
            if (!valued.count(key)) { err = "unknown option: --" + key; return false; }
            if (!has_val) {
                if (i + 1 >= argc) { err = "option --" + key + " needs a value"; return false; }
                val = argv[++i];
            }
            vals_[key] = val;
        }
        return true;
    }
    bool has(const std::string& k) const { return vals_.count(k) != 0; }
    std::string str(const std::string& k, const std::string& def) const {
        auto it = vals_.find(k);
        return it == vals_.end() ? def : it->second;
    }
    bool u64(const std::string& k, uint64_t def, uint64_t& out, std::string& err) const {
        auto it = vals_.find(k);
        if (it == vals_.end()) { out = def; return true; }
        errno = 0;
        char* end = nullptr;
        const unsigned long long v = std::strtoull(it->second.c_str(), &end, 10);
        if (errno != 0 || end == it->second.c_str() || *end != '\0' || it->second[0] == '-') {
            err = "bad value for --" + k + ": " + it->second;
            return false;
        }
        out = v;
        return true;
    }
private:
    std::map<std::string, std::string> vals_;
};

// `arg` is a YAML file path or an inline YAML string. The document may be the
// framing map itself, a map with a `framing:` key, or a full config.yaml
// (partitions[0].framing is used). Empty arg = default (bare concatenation).
inline bool load_framing(const std::string& arg, flashtick::FramingConfig& out, std::string& err) {
    if (arg.empty()) return flashtick::FramingConfig::parse(YAML::Node(), out, err);
    YAML::Node root;
    try {
        std::error_code ec;
        root = std::filesystem::exists(arg, ec) ? YAML::LoadFile(arg) : YAML::Load(arg);
    } catch (const YAML::Exception& e) {
        err = std::string("framing yaml: ") + e.what();
        return false;
    }
    const YAML::Node& r = root;
    YAML::Node node = root;
    try {
        if (r.IsMap()) {
            if (r["framing"]) {
                node = r["framing"];
            } else if (r["partitions"] && r["partitions"].IsSequence() && r["partitions"].size() > 0 &&
                       r["partitions"][0]["framing"]) {
                node = r["partitions"][0]["framing"];
            }
        }
    } catch (const YAML::Exception& e) {
        err = std::string("framing yaml: ") + e.what();
        return false;
    }
    return flashtick::FramingConfig::parse(node, out, err);
}

// Not movable: Framer keeps a pointer to the walker.
struct LoadedSchema {
    flashtick::SbeSchema schema;
    flashtick::SbeLengthWalker walker;
    LoadedSchema() = default;
    LoadedSchema(const LoadedSchema&) = delete;
    LoadedSchema& operator=(const LoadedSchema&) = delete;
    bool load(const std::string& path, std::string& err) {
        if (!schema.load_file(path, err)) return false;
        return walker.build(schema, err);
    }
};

// Monotonic clock for pacing.
inline uint64_t mono_ns() {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
}

// Fixed-rate pacer: sleep until the next deadline (spinning the last 50 us).
// interval_ns == 0 disables pacing. `stop` aborts the wait early.
class Pacer {
public:
    explicit Pacer(uint64_t interval_ns) : interval_(interval_ns), deadline_(mono_ns()) {}
    void wait(const volatile std::sig_atomic_t& stop) {
        if (interval_ == 0) return;
        deadline_ += interval_;
        for (;;) {
            const uint64_t now = mono_ns();
            if (now >= deadline_ || stop) break;
            if (deadline_ - now > 100000) std::this_thread::sleep_for(std::chrono::nanoseconds(deadline_ - now - 50000));
        }
    }
private:
    uint64_t interval_, deadline_;
};

} // namespace fttool
