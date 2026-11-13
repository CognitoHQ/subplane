#include "subplane/recap/recap_config.h"

namespace subplane {

bool RecapConfig::parse(const YAML::Node& node, RecapConfig& out, std::string& err) {
    try {
        out = RecapConfig{};
        if (!node.IsMap()) { err = "recap must be a map"; return false; }
        out.type = node["type"].as<std::string>("");
        if (out.type.empty()) { err = "recap.type is required"; return false; }
        const std::string reply = node["reply"].as<std::string>("spp");
        if (reply == "spp") out.reply = RecapReplyMode::SPP;
        else if (reply == "stream_markers") out.reply = RecapReplyMode::STREAM_MARKERS;
        else { err = "recap.reply must be spp or stream_markers, not '" + reply + "'"; return false; }
        out.timeout_ms = node["timeout_ms"].as<uint32_t>(out.timeout_ms);
        out.max_inflight = node["max_inflight"].as<uint32_t>(out.max_inflight);
        out.coalesce_ms = node["coalesce_ms"].as<uint32_t>(out.coalesce_ms);
        if (const auto r = node["retry"]) {
            if (!r.IsMap()) { err = "recap.retry must be {base_ms, max_ms}"; return false; }
            out.retry_base_ms = r["base_ms"].as<uint32_t>(out.retry_base_ms);
            out.retry_max_ms = r["max_ms"].as<uint32_t>(out.retry_max_ms);
        }
        out.reply_slabs = node["reply_slabs"].as<uint32_t>(out.reply_slabs);
        out.reply_slab_bytes = node["reply_slab_bytes"].as<uint32_t>(out.reply_slab_bytes);
        out.max_pending = node["max_pending"].as<uint32_t>(out.max_pending);
        out.hold_bytes = node["hold_bytes"].as<uint64_t>(out.hold_bytes);
        out.hold_chunk_bytes = node["hold_chunk_bytes"].as<uint32_t>(out.hold_chunk_bytes);
    } catch (const YAML::Exception& e) {
        err = std::string("recap config: ") + e.what();
        return false;
    }
    if (out.timeout_ms == 0) { err = "recap.timeout_ms must be > 0"; return false; }
    if (out.max_inflight == 0 || out.max_inflight > 4096) { err = "recap.max_inflight must be 1..4096"; return false; }
    if (out.coalesce_ms > 10000) { err = "recap.coalesce_ms must be 0..10000"; return false; }
    if (out.retry_base_ms == 0 || out.retry_max_ms < out.retry_base_ms) { err = "recap.retry needs 0 < base_ms <= max_ms"; return false; }
    if (out.reply_slabs == 0 || out.reply_slabs > 1024) { err = "recap.reply_slabs must be 1..1024"; return false; }
    if (out.reply_slab_bytes < 4096 || out.reply_slab_bytes > (256u << 20)) { err = "recap.reply_slab_bytes must be 4096..268435456"; return false; }
    if (out.max_pending == 0 || out.max_pending > (1u << 20)) { err = "recap.max_pending must be 1..1048576"; return false; }
    if (out.hold_chunk_bytes < 64 || out.hold_chunk_bytes > 65536) { err = "recap.hold_chunk_bytes must be 64..65536"; return false; }
    if (out.hold_bytes < out.hold_chunk_bytes || out.hold_bytes > (16ull << 30) ||
        out.hold_bytes / out.hold_chunk_bytes >= 0xFFFFFFFFull) {
        err = "recap.hold_bytes must be hold_chunk_bytes..16 GiB"; return false;
    }
    return true;
}

bool ResolverConfig::parse(const YAML::Node& node, ResolverConfig& out, std::string& err) {
    try {
        out = ResolverConfig{};
        if (!node.IsMap()) { err = "resolver must be a map"; return false; }
        out.type = node["type"].as<std::string>("");
        if (out.type.empty()) { err = "resolver.type is required"; return false; }
        if (node["sections"].IsDefined()) { err = "resolver has no section stream; remove `sections:`"; return false; }
        out.timeout_ms = node["timeout_ms"].as<uint32_t>(out.timeout_ms);
        out.refresh_ms = node["refresh_ms"].as<uint32_t>(out.refresh_ms);
    } catch (const YAML::Exception& e) {
        err = std::string("resolver config: ") + e.what();
        return false;
    }
    if (out.timeout_ms == 0) { err = "resolver.timeout_ms must be > 0"; return false; }
    if (out.refresh_ms < out.timeout_ms) { err = "resolver.refresh_ms must be at least timeout_ms"; return false; }
    return true;
}

} // namespace subplane
