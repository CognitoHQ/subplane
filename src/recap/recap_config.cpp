#include "subplane/recap/recap_config.h"

namespace subplane {

bool RecapConfig::parse(const YAML::Node& node, RecapConfig& out, std::string& err) {
    try {
        out = RecapConfig{};
        if (!node.IsMap()) { err = "recap must be a map"; return false; }
        out.type = node["type"].as<std::string>("");
        if (out.type.empty()) { err = "recap.type is required"; return false; }
        out.timeout_ms = node["timeout_ms"].as<uint32_t>(out.timeout_ms);
        out.max_inflight = node["max_inflight"].as<uint32_t>(out.max_inflight);
        if (const auto r = node["retry"]) {
            if (!r.IsMap()) { err = "recap.retry must be {base_ms, max_ms}"; return false; }
            out.retry_base_ms = r["base_ms"].as<uint32_t>(out.retry_base_ms);
            out.retry_max_ms = r["max_ms"].as<uint32_t>(out.retry_max_ms);
        }
        out.reply_slabs = node["reply_slabs"].as<uint32_t>(out.reply_slabs);
        out.reply_slab_bytes = node["reply_slab_bytes"].as<uint32_t>(out.reply_slab_bytes);
    } catch (const YAML::Exception& e) {
        err = std::string("recap config: ") + e.what();
        return false;
    }
    if (out.timeout_ms == 0) { err = "recap.timeout_ms must be > 0"; return false; }
    if (out.max_inflight == 0 || out.max_inflight > 4096) { err = "recap.max_inflight must be 1..4096"; return false; }
    if (out.retry_base_ms == 0 || out.retry_max_ms < out.retry_base_ms) { err = "recap.retry needs 0 < base_ms <= max_ms"; return false; }
    if (out.reply_slabs == 0 || out.reply_slabs > 1024) { err = "recap.reply_slabs must be 1..1024"; return false; }
    if (out.reply_slab_bytes < 4096 || out.reply_slab_bytes > (256u << 20)) { err = "recap.reply_slab_bytes must be 4096..268435456"; return false; }
    return true;
}

} // namespace subplane
