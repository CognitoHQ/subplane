#include "subplane/pipeline/partition.h"

#include "subplane/registry/component_registry.h"

namespace subplane {

const char* decoder_type_name(DecoderType t) {
    switch (t) {
        case DecoderType::SBE_ROUTING: return "sbe_routing";
        case DecoderType::DBN:         return "dbn";
        case DecoderType::FLATBUFFERS: return "flatbuffers";
    }
    return "unknown";
}

const char* partition_state_name(PartitionState s) {
    switch (s) {
        case PartitionState::INIT:     return "init";
        case PartitionState::ACTIVE:   return "active";
        case PartitionState::DEGRADED: return "degraded";
    }
    return "unknown";
}

bool PartitionConfig::parse(const YAML::Node& node, uint16_t id, const std::string& base_dir,
                            PartitionConfig& out, std::string& err) {
    out = PartitionConfig{};
    out.id = id;
    out.base_dir = base_dir;
    try {
        out.name = node["name"].as<std::string>("");
        if (out.name.empty()) {
            err = "partitions[" + std::to_string(id) + "].name is required";
            return false;
        }
        if (!node["transport"].IsDefined() || !node["transport"].IsMap()) {
            err = "partition " + out.name + ": transport block is required";
            return false;
        }
        out.transport_type = node["transport"]["type"].as<std::string>("");
        if (out.transport_type.empty()) {
            err = "partition " + out.name + ": transport.type is required";
            return false;
        }
        // INTENT: yaml-cpp throws InvalidNode when assigning an undefined child
        // (missing key) to a Node. Clone only what exists; see lessons.md.
        out.transport_cfg = YAML::Clone(node["transport"]);
        if (node["framing"].IsDefined()) out.framing_node = YAML::Clone(node["framing"]);
        if (!node["decoder"].IsDefined() || !node["decoder"].IsMap()) {
            err = "partition " + out.name + ": decoder block is required";
            return false;
        }
        out.decoder_node = YAML::Clone(node["decoder"]);
        out.heartbeat_timeout_ms = node["heartbeat_timeout_ms"].as<uint32_t>(2000u);
        out.dup_window = node["dup_window"].as<uint64_t>(1024u);
        if (const auto l = node["lvc"]) {
            bool on = true;
            if (!l.IsScalar() || !YAML::convert<bool>::decode(l, on)) {
                err = "partition " + out.name + ": lvc must be on or off";
                return false;
            }
            out.lvc = on;
        }
    } catch (const YAML::Exception& e) {
        err = "partition " + out.name + ": " + e.what();
        return false;
    }
    return true;
}

bool Partition::build(const PartitionConfig& cfg, std::string& err, std::unique_ptr<Transport> transport_override) {
    cfg_ = cfg;
    dup_window = cfg.dup_window;
    lvc_enabled = cfg.lvc;
    if (cfg_.id >= 64) {
        err = "partition " + cfg_.name + ": at most 64 partitions per agent (id " + std::to_string(cfg_.id) + ")";
        return false;
    }

    // Encoding is per-partition configuration: `decoder.type` selects the
    // decoder; the transport never knows what it carries.
    std::string dtype = "sbe_routing";
    try {
        if (cfg_.decoder_node && cfg_.decoder_node.IsMap() && cfg_.decoder_node["type"])
            dtype = cfg_.decoder_node["type"].as<std::string>();
    } catch (const YAML::Exception& e) {
        err = "partition " + cfg_.name + ": decoder.type: " + e.what();
        return false;
    }
    const SbeLengthWalker* walker = nullptr;
    if (dtype == "sbe_routing") {
        decoder_type_ = DecoderType::SBE_ROUTING;
        DecoderConfig dcfg;
        if (!parse_decoder_config(cfg_.decoder_node, dcfg, err)) {
            err = "partition " + cfg_.name + ": " + err;
            return false;
        }
        std::string schema_path = dcfg.schema_path;
        if (!schema_path.empty() && schema_path[0] != '/' && !cfg_.base_dir.empty()) {
            schema_path = cfg_.base_dir + "/" + schema_path;
        }
        if (!schema_.load_file(schema_path, err)) {
            err = "partition " + cfg_.name + ": schema " + schema_path + ": " + err;
            return false;
        }
        if (!sbe_decoder_.init(dcfg, schema_, err)) {
            err = "partition " + cfg_.name + ": decoder: " + err;
            return false;
        }
        walker = &sbe_decoder_.walker();
    } else if (dtype == "dbn") {
        decoder_type_ = DecoderType::DBN;
        DbnDecoderConfig dcfg;
        if (!parse_dbn_decoder_config(cfg_.decoder_node, dcfg, err) || !dbn_decoder_.init(dcfg, err)) {
            err = "partition " + cfg_.name + ": decoder: " + err;
            return false;
        }
    } else if (dtype == "flatbuffers") {
        decoder_type_ = DecoderType::FLATBUFFERS;
        FlatbuffersDecoderConfig dcfg;
        if (!parse_flatbuffers_decoder_config(cfg_.decoder_node, dcfg, err) || !fb_decoder_.init(dcfg, err)) {
            err = "partition " + cfg_.name + ": decoder: " + err;
            return false;
        }
    } else {
        err = "partition " + cfg_.name + ": unknown decoder type '" + dtype + "'; available:";
        for (const char* t : decoder_types()) { err += ' '; err += t; }
        return false;
    }

    FramingConfig fcfg;
    YAML::Node framing_node = cfg_.framing_node;
    // INTENT: a default-constructed YAML::Node is a defined Null node, so test
    // both; an absent `framing:` block looks like either depending on the path.
    if ((!framing_node.IsDefined() || framing_node.IsNull()) &&
        (decoder_type_ == DecoderType::DBN || decoder_type_ == DecoderType::FLATBUFFERS)) {
        // DBN records and FlatBuffers carry their own length and arrive one per
        // packet (or per tcp_stream record) unless the framing says otherwise.
        framing_node = YAML::Load("{length: packet}");
    }
    if (!FramingConfig::parse(framing_node, fcfg, err)) {
        err = "partition " + cfg_.name + ": " + err;
        return false;
    }
    if (!framer_.init(fcfg, walker, err)) {
        err = "partition " + cfg_.name + ": " + err;
        return false;
    }

    if (transport_override) {
        transport_ = std::move(transport_override);
    } else {
        transport_ = make_component(transport_components(), cfg_.transport_type, cfg_.transport_cfg, err);
        if (!transport_) {
            err = "partition " + cfg_.name + ": transport: " + err;
            return false;
        }
    }
    return true;
}

bool Partition::open(std::string& err) {
    if (!transport_) {
        err = "partition " + cfg_.name + ": not built";
        return false;
    }
    if (!transport_->open(err)) {
        err = "partition " + cfg_.name + ": " + err;
        return false;
    }
    return true;
}

void Partition::close() noexcept {
    if (transport_) transport_->close();
}

bool Partition::attach(Session* s) noexcept {
    for (uint32_t i = 0; i < n_sessions; ++i) {
        if (sessions[i] == s) return false;
    }
    if (n_sessions >= kMaxSessions) return false;
    sessions[n_sessions++] = s;
    return true;
}

bool Partition::detach(Session* s) noexcept {
    for (uint32_t i = 0; i < n_sessions; ++i) {
        if (sessions[i] == s) {
            sessions[i] = sessions[n_sessions - 1];
            sessions[n_sessions - 1] = nullptr;
            --n_sessions;
            return true;
        }
    }
    return false;
}

} // namespace subplane
