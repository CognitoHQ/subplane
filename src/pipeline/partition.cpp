#include "subplane/pipeline/partition.h"
#include "subplane/pipeline/topic_registry.h"

#include "subplane/record/pcapng.h"
#include "subplane/record/sha256.h"
#include "subplane/registry/component_registry.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <iterator>

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
            err = "partition " + std::to_string(id) + ": name is required";
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
        // (missing key) to a Node. Clone only what exists.
        out.transport_cfg = YAML::Clone(node["transport"]);
        if (node["framing"].IsDefined()) out.framing_node = YAML::Clone(node["framing"]);
        if (!node["decoder"].IsDefined() || !node["decoder"].IsMap()) {
            err = "partition " + out.name + ": decoder block is required";
            return false;
        }
        out.decoder_node = YAML::Clone(node["decoder"]);
        if (node["recap"].IsDefined()) out.recap_node = YAML::Clone(node["recap"]);
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
        if (const auto t = node["tap"]) {
            if (!t.IsMap()) {
                err = "partition " + out.name + ": tap must be a map, e.g. tap: { capacity_mb: 256 }";
                return false;
            }
            out.tap = true;
            out.tap_capacity_bytes = t["capacity_mb"].as<uint64_t>(256u) << 20;
            out.tap_max_payload = t["max_payload"].as<uint32_t>(65536u);
            const uint64_t c = out.tap_capacity_bytes;
            if (c < (64ull << 10) || (c & (c - 1)) != 0) {
                err = "partition " + out.name + ": tap.capacity_mb must be a power of two";
                return false;
            }
            if (out.tap_max_payload == 0 || out.tap_max_payload > c / 4) {
                err = "partition " + out.name + ": tap.max_payload must be in [1, capacity/4]";
                return false;
            }
        }
        YAML::Emitter e;
        e << node;
        out.yaml = e.c_str();
    } catch (const YAML::Exception& e) {
        err = "partition " + out.name + ": " + e.what();
        return false;
    }
    return true;
}

void PartitionConfig::set_topic(std::string topic, uint16_t tid, uint16_t num) {
    topic_name = std::move(topic);
    topic_id = tid;
    partition_num = num;
    stream_id = stream_id_of(tid, num);
}

namespace {
// Deep merge for topic expansion: maps merge key by key, anything else in
// `over` replaces what `base` has.
YAML::Node merge_nodes(const YAML::Node& base, const YAML::Node& over) {
    if (!over.IsDefined() || over.IsNull()) return YAML::Clone(base);
    if (!base.IsDefined() || !base.IsMap() || !over.IsMap()) return YAML::Clone(over);
    YAML::Node out = YAML::Clone(base);
    for (const auto& kv : over) {
        const std::string k = kv.first.as<std::string>();
        out[k] = merge_nodes(base[k], kv.second);
    }
    return out;
}
} // namespace

YAML::Node topic_partition_node(const YAML::Node& tn, size_t index) {
    if (!tn.IsMap()) return YAML::Node(YAML::NodeType::Undefined);
    YAML::Node base(YAML::NodeType::Map);
    for (const auto& kv : tn) {
        const std::string k = kv.first.as<std::string>();
        if (k != "name" && k != "partitions") base[k] = YAML::Clone(kv.second);
    }
    // INTENT: a topic-level `transport.driver` starts one embedded media driver
    // for the topic, so only partition 1 inherits it; the others use that driver.
    // Inherited by every partition, partition 2 would ask for a second driver on
    // the same directory and the agent would refuse to start.
    if (index != 0 && base["transport"].IsMap() && base["transport"]["driver"].IsDefined()) base["transport"].remove("driver");
    YAML::Node entry(YAML::NodeType::Map);
    if (const auto ps = tn["partitions"]; ps.IsDefined()) {
        if (!ps.IsSequence() || index >= ps.size()) return YAML::Node(YAML::NodeType::Undefined);
        entry = ps[index];
    } else if (index != 0) {
        return YAML::Node(YAML::NodeType::Undefined);
    }
    YAML::Node merged = merge_nodes(base, entry);
    merged["name"] = tn["name"].as<std::string>("") + "/" + std::to_string(index + 1);
    return merged;
}

bool expand_topics(const YAML::Node& topics, const TopicRegistry& registry, const std::string& base_dir,
                   std::vector<PartitionConfig>& out, std::string& err) {
    out.clear();
    if (!topics.IsSequence()) {
        err = "topics must be a sequence";
        return false;
    }
    try {
        for (const auto& tn : topics) {
            const std::string tname = tn["name"].as<std::string>("");
            if (!valid_topic_name(tname, err)) return false;
            const TopicEntry* te = registry.find(tname);
            if (te == nullptr) {
                err = "topic " + tname + " is not in the topic registry (`registry:`)";
                return false;
            }
            for (const auto& pc : out) {
                if (pc.topic_name == tname) {
                    err = "topic " + tname + " is listed twice";
                    return false;
                }
            }
            if (tn["id"].IsDefined()) {
                err = "topic " + tname + ": the id comes from the registry; remove `id:` here";
                return false;
            }
            size_t count = 1;
            if (const auto ps = tn["partitions"]; ps.IsDefined()) {
                if (!ps.IsSequence() || ps.size() == 0) {
                    err = "topic " + tname + ": partitions is a non-empty sequence (one entry per partition, from 1)";
                    return false;
                }
                for (const auto& e : ps) {
                    if (!e.IsMap() && !e.IsNull()) {
                        err = "topic " + tname + ": each partitions entry is a map ({} for no overrides)";
                        return false;
                    }
                    if (e.IsMap() && e["name"].IsDefined()) {
                        err = "topic " + tname + ": a partition has no name of its own (it is " + tname + "/<n>)";
                        return false;
                    }
                }
                count = ps.size();
            }
            if (count > 65535) {
                err = "topic " + tname + ": at most 65535 partitions";
                return false;
            }
            for (size_t i = 0; i < count; ++i) {
                const auto num = static_cast<uint16_t>(i + 1);
                YAML::Node merged = topic_partition_node(tn, i);
                const int32_t sid = stream_id_of(te->id, num);
                if (merged["transport"].IsMap() && merged["transport"]["type"].as<std::string>("") == "aeron" &&
                    !merged["transport"]["stream_id"].IsDefined()) {
                    merged["transport"]["stream_id"] = sid;
                }
                PartitionConfig cfg;
                if (!PartitionConfig::parse(merged, static_cast<uint16_t>(out.size()), base_dir, cfg, err)) return false;
                cfg.set_topic(tname, te->id, num);
                out.push_back(std::move(cfg));
            }
        }
    } catch (const YAML::Exception& e) {
        err = std::string("topics: ") + e.what();
        return false;
    }
    return true;
}

bool Partition::build(const PartitionConfig& cfg, std::string& err, std::unique_ptr<Transport> transport_override,
                      std::unique_ptr<RecapTransport> recap_override) {
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
        if (cfg_.tap) { // the recording carries the schema the decoder loaded
            std::ifstream f(schema_path, std::ios::binary);
            schema_xml_.assign(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
        }
        if (!sbe_decoder_.init(dcfg, schema_, err)) {
            err = "partition " + cfg_.name + ": decoder: " + err;
            return false;
        }
        // INTENT: the instrument state is an array indexed by symbol id, so
        // the ids must be the table's dense ones; passthrough ids are any u32.
        if (sbe_decoder_.maps_instrument_sequence() && sbe_decoder_.symbol_passthrough()) {
            err = "partition " + cfg_.name + ": decoder: instrument_sequence needs symbol_id_mode: table";
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

    if (!build_recap(std::move(recap_override), err)) return false;
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

bool Partition::build_recap(std::unique_ptr<RecapTransport> recap_override, std::string& err) {
    has_recap_ = false;
    if (!cfg_.recap_node.IsDefined() || cfg_.recap_node.IsNull()) return true;
    const std::string pre = "partition " + cfg_.name + ": recap: ";
    if (!RecapConfig::parse(cfg_.recap_node, recap_cfg_, err)) { err = pre + err; return false; }
    // INTENT: the request names the instrument as a u64, so the symbol must
    // be an integer. In `reply: stream_markers` mode the reply also comes back
    // in the stream's own schema between markers the decoder must recognise;
    // in `spp` mode records travel as opaque SPP payloads and any decoder can
    // recap (DBN included).
    const bool markers = recap_cfg_.reply == RecapReplyMode::STREAM_MARKERS;
    bool begin = false, end = false, str = false;
    switch (decoder_type_) {
        case DecoderType::DBN:
            if (markers) { err = pre + "type dbn cannot use reply: stream_markers (DBN has no recap marker records); use reply: spp"; return false; }
            break;
        case DecoderType::FLATBUFFERS:
            begin = fb_decoder_.maps_marker(RecapMarker::BEGIN);
            end = fb_decoder_.maps_marker(RecapMarker::END);
            str = fb_decoder_.has_string_symbol();
            break;
        default:
            begin = sbe_decoder_.maps_marker(RecapMarker::BEGIN);
            end = sbe_decoder_.maps_marker(RecapMarker::END);
            str = sbe_decoder_.has_string_symbol();
            break;
    }
    if (markers && (!begin || !end)) { err = pre + "reply: stream_markers needs the decoder to map RECAP_BEGIN and RECAP_END (decoder.messages, admin: RECAP_BEGIN)"; return false; }
    if (str) { err = pre + "needs an integer symbol field; the request names the instrument as a u64"; return false; }
    // INTENT: the merge places a snapshot "as of S" against live messages by
    // their sequence; without one there is no way to tell which held messages
    // the snapshot already contains.
    if (framer_.seq_source() == SeqSource::NONE) { err = pre + "needs a sequence (framing.sequence) to merge snapshots with live data"; return false; }
    if (recap_override) {
        recap_transport_ = std::move(recap_override);
    } else {
        recap_transport_ = make_component(recap_components(), recap_cfg_.type, cfg_.recap_node, err);
        if (!recap_transport_) { err = pre + err; return false; }
    }
    has_recap_ = true;
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
    if (tap_ring_) tap_ring_->close();
}

namespace {

// Where a recorder puts this partition's units in their synthetic UDP header
// (recording-format.md): the group or host when it is an IPv4 address, else
// 127.0.0.1, and the port. An Aeron stream has neither: its 32-bit stream id
// (topic_id << 16 | partition_num by default) is spread over both, 127.0.x.y
// with x.y its high 16 bits and the port its low 16, so no two streams share
// an address.
void tap_wrap(const std::string& type, const YAML::Node& t, uint32_t& ip, uint16_t& port) {
    ip = 0x7F000001u;
    port = 0;
    std::string addr;
    try {
        if (type == "aeron") {
            const auto sid = static_cast<uint32_t>(t["stream_id"].as<int32_t>(0));
            ip = 0x7F000000u | (sid >> 16);
            port = static_cast<uint16_t>(sid & 0xFFFFu);
            return;
        }
        port = static_cast<uint16_t>(t["port"].as<int>(0));
        addr = t[type == "tcp_stream" ? "host" : "group"].as<std::string>("");
    } catch (const YAML::Exception&) {
    }
    in_addr a{};
    if (!addr.empty() && ::inet_pton(AF_INET, addr.c_str(), &a) == 1) ip = ntohl(a.s_addr);
}

bool write_file_atomic(const std::string& path, const std::vector<uint8_t>& bytes, uint32_t mode, std::string& err) {
    const std::string tmp = path + ".tmp";
    ::unlink(tmp.c_str());
    const int fd = ::open(tmp.c_str(), O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0644);
    if (fd < 0) { err = "open " + tmp + ": " + std::strerror(errno); return false; }
    bool ok = ::fchmod(fd, static_cast<mode_t>(mode & 0666u)) == 0;
    size_t off = 0;
    while (ok && off < bytes.size()) {
        const ssize_t n = ::write(fd, bytes.data() + off, bytes.size() - off);
        if (n < 0 && errno == EINTR) continue;
        ok = n > 0;
        if (ok) off += static_cast<size_t>(n);
    }
    if (::close(fd) != 0) ok = false;
    if (!ok || ::rename(tmp.c_str(), path.c_str()) != 0) {
        err = "write " + path + ": " + std::strerror(errno);
        ::unlink(tmp.c_str());
        return false;
    }
    return true;
}

} // namespace

bool Partition::open_tap(const TapSetup& setup, std::string& err) {
    if (!cfg_.tap) return true;
    using namespace record;
    // INTENT: the file name spells "<topic>/<n>" as "<topic>.<n>": a '/' would
    // be a directory. Still one name per partition: the last segment is the number.
    std::string file_name = cfg_.name;
    std::replace(file_name.begin(), file_name.end(), '/', '.');
    const std::string base = setup.ring_dir + "/subplane." + setup.agent_name + ".tap." + file_name;
    tap_path_ = base + ".ring";
    tap_ctx_path_ = base + ".ctx";

    // The CONTEXT block body, as the recorder writes it at the head of every segment.
    std::vector<uint8_t> ctx;
    tlv_put_u32(ctx, SpRecTag::FORMAT_VERSION, kSpRecFormatVersion);
    tlv_put(ctx, SpRecTag::AGENT_NAME, setup.agent_name);
    tlv_put(ctx, SpRecTag::AGENT_VERSION, setup.agent_version);
    tlv_put_u64(ctx, SpRecTag::AGENT_EPOCH, setup.epoch);
    tlv_put(ctx, SpRecTag::PARTITION, cfg_.name);
    tlv_put(ctx, SpRecTag::TRANSPORT_TYPE, cfg_.transport_type);
    tlv_put(ctx, SpRecTag::PARTITION_YAML, cfg_.yaml);
    const Sha256Digest yh = sha256(cfg_.yaml);
    tlv_put(ctx, SpRecTag::PARTITION_SHA256, yh.data(), uint32_t(yh.size()));
    if (!schema_xml_.empty()) {
        tlv_put(ctx, SpRecTag::SCHEMA_XML, schema_xml_);
        const Sha256Digest sh = sha256(schema_xml_);
        tlv_put(ctx, SpRecTag::SCHEMA_SHA256, sh.data(), uint32_t(sh.size()));
    }
    bool kernel_ts = false;
    try {
        kernel_ts = cfg_.transport_cfg["kernel_timestamps"].as<bool>(false);
    } catch (const YAML::Exception&) {
    }
    tlv_put(ctx, SpRecTag::TS_SOURCE, kernel_ts ? "kernel" : "poll");
    uint32_t ip = 0;
    uint16_t port = 0;
    tap_wrap(cfg_.transport_type, cfg_.transport_cfg, ip, port);
    uint8_t wrap[8] = {};
    std::memcpy(wrap, &ip, 4);
    std::memcpy(wrap + 4, &port, 2);
    tlv_put(ctx, SpRecTag::WRAP, wrap, sizeof wrap);
    if (!write_file_atomic(tap_ctx_path_, ctx, setup.ring_mode, err)) {
        err = "partition " + cfg_.name + ": tap context: " + err;
        return false;
    }

    RingConfig rc;
    rc.capacity_bytes = cfg_.tap_capacity_bytes;
    rc.max_payload = cfg_.tap_max_payload;
    rc.session_name = ("tap." + file_name).substr(0, sizeof(RingHeader::session_name) - 1);
    rc.mode = setup.ring_mode;
    tap_ring_ = std::make_unique<RingWriter>();
    if (!tap_ring_->open(rc, tap_path_, setup.epoch, err)) {
        err = "partition " + cfg_.name + ": tap ring " + tap_path_ + ": " + err;
        tap_ring_.reset();
        return false;
    }
    tap = tap_ring_.get();
    tap_seq = 0;
    return true;
}

bool Partition::attach(Session* s) noexcept {
    if (std::ranges::contains(sessions, s)) return false;
    return sessions.try_push_back(s) != nullptr;
}

bool Partition::detach(Session* s) noexcept {
    const auto it = std::ranges::find(sessions, s);
    if (it == sessions.end()) return false;
    *it = sessions.back();   // order does not matter; recap uses slots, not indices
    sessions.pop_back();
    return true;
}

} // namespace subplane
