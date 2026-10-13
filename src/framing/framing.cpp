#include "subplane/framing/framing.h"

#include <bit>
#include <cstring>

namespace subplane {

namespace {

bool parse_type(const std::string& s, FieldType& t, uint32_t& size) {
    if (s == "u8")    { t = FieldType::U8;    size = 1; return true; }
    if (s == "u16le") { t = FieldType::U16LE; size = 2; return true; }
    if (s == "u16be") { t = FieldType::U16BE; size = 2; return true; }
    if (s == "u32le") { t = FieldType::U32LE; size = 4; return true; }
    if (s == "u32be") { t = FieldType::U32BE; size = 4; return true; }
    if (s == "u64le") { t = FieldType::U64LE; size = 8; return true; }
    if (s == "u64be") { t = FieldType::U64BE; size = 8; return true; }
    if (s == "bytes") { t = FieldType::BYTES; size = 0; return true; }
    return false;
}

bool parse_fields(const YAML::Node& node, const char* section, std::vector<FramingField>& out,
                  uint32_t& total, std::string& err) {
    out.clear();
    total = 0;
    if (!node.IsDefined() || node.IsNull()) {
        return true;
    }
    if (!node.IsSequence()) {
        err = std::string("framing.") + section + " must be a sequence";
        return false;
    }
    uint32_t cursor = 0;
    for (const auto& f : node) {
        FramingField ff;
        ff.name = f["name"].as<std::string>("");
        std::string type = f["type"].as<std::string>("");
        if (ff.name.empty() || !parse_type(type, ff.type, ff.size)) {
            err = std::string("framing.") + section + ": each field needs a name and a type in "
                  "{u8,u16le,u16be,u32le,u32be,u64le,u64be,bytes}";
            return false;
        }
        if (ff.type == FieldType::BYTES) {
            ff.size = f["size"].as<uint32_t>(0);
            if (ff.size == 0) {
                err = std::string("framing.") + section + "." + ff.name + ": bytes needs size > 0";
                return false;
            }
        }
        ff.offset = f["offset"].IsDefined() ? f["offset"].as<uint32_t>() : cursor;
        cursor = ff.offset + ff.size;
        total = cursor > total ? cursor : total;
        for (const auto& prev : out) {
            if (prev.name == ff.name) {
                err = std::string("framing.") + section + ": duplicate field " + ff.name;
                return false;
            }
        }
        out.push_back(std::move(ff));
    }
    return true;
}

const FramingField* find(const std::vector<FramingField>& v, const std::string& name) {
    for (const auto& f : v) {
        if (f.name == name) return &f;
    }
    return nullptr;
}

bool split_ref(const std::string& ref, std::string& scope, std::string& name) {
    size_t dot = ref.find('.');
    if (dot == std::string::npos) return false;
    scope = ref.substr(0, dot);
    name = ref.substr(dot + 1);
    return !scope.empty() && !name.empty();
}

} // namespace

bool FramingConfig::parse(const YAML::Node& node, FramingConfig& out, std::string& err) {
    out = FramingConfig{};
    if (!node.IsDefined() || node.IsNull()) {
        return true;
    }
    if (!node.IsMap()) {
        err = "framing must be a map";
        return false;
    }
    try {
        if (!parse_fields(node["packet_header"], "packet_header", out.packet_header, out.packet_header_size, err)) return false;
        if (!parse_fields(node["message_prefix"], "message_prefix", out.message_prefix, out.message_prefix_size, err)) return false;

        std::string seq = node["sequence"].as<std::string>("none");
        if (seq == "none") {
            out.seq_source = SeqSource::NONE;
        } else if (seq == "message") {
            out.seq_source = SeqSource::MESSAGE_FIELD; // the decoder fills TickHeader::sequence itself
        } else {
            std::string scope, name;
            if (!split_ref(seq, scope, name)) {
                err = "framing.sequence must be none, packet_header.<field>, message or message.<SchemaField>";
                return false;
            }
            if (scope == "packet_header") {
                const auto* f = find(out.packet_header, name);
                if (!f || f->type == FieldType::BYTES) {
                    err = "framing.sequence refers to unknown or non-integer packet_header field " + name;
                    return false;
                }
                out.seq_source = SeqSource::PACKET_HEADER;
            } else if (scope == "message") {
                out.seq_source = SeqSource::MESSAGE_FIELD;
            } else {
                err = "framing.sequence scope must be packet_header or message";
                return false;
            }
            out.seq_field = name;
        }

        std::string count = node["count"].as<std::string>("");
        if (!count.empty()) {
            std::string scope, name;
            if (!split_ref(count, scope, name) || scope != "packet_header" || !find(out.packet_header, name) ||
                find(out.packet_header, name)->type == FieldType::BYTES) {
                err = "framing.count must be packet_header.<integer field>";
                return false;
            }
            out.has_count = true;
            out.count_field = name;
        }

        std::string len = node["length"].as<std::string>("schema");
        if (len == "schema") {
            out.len_source = LenSource::SCHEMA;
        } else if (len == "packet") {
            out.len_source = LenSource::PACKET;
            if (!out.message_prefix.empty()) { err = "framing.length packet cannot be combined with message_prefix"; return false; }
        } else {
            std::string scope, name;
            if (!split_ref(len, scope, name) || scope != "message_prefix" || !find(out.message_prefix, name) ||
                find(out.message_prefix, name)->type == FieldType::BYTES) {
                err = "framing.length must be schema, packet or message_prefix.<integer field>";
                return false;
            }
            out.len_source = LenSource::MESSAGE_PREFIX;
            out.len_field = name;
        }
        out.length_includes_prefix = node["length_includes_prefix"].as<bool>(false);
        out.keep_prefix = node["keep_prefix"].as<bool>(false);
        if (out.keep_prefix && out.message_prefix.empty()) { err = "framing.keep_prefix needs a message_prefix"; return false; }
    } catch (const YAML::Exception& e) {
        err = std::string("framing: ") + e.what();
        return false;
    }
    return true;
}

bool Framer::init(const FramingConfig& cfg, const SbeLengthWalker* walker, std::string& err) {
    cfg_ = cfg;
    walker_ = walker;
    if (cfg_.len_source == LenSource::SCHEMA && walker_ == nullptr) {
        err = "framing.length is schema but this decoder cannot compute message length; use length: packet or a message_prefix";
        return false;
    }
    auto bind = [](const std::vector<FramingField>& v, const std::string& name, Accessor& a) {
        const auto* f = find(v, name);
        if (!f) return false;
        a.offset = f->offset;
        a.size = static_cast<uint8_t>(f->size);
        a.big = f->type == FieldType::U16BE || f->type == FieldType::U32BE || f->type == FieldType::U64BE;
        a.present = true;
        return true;
    };
    if (cfg_.seq_source == SeqSource::PACKET_HEADER && !bind(cfg_.packet_header, cfg_.seq_field, seq_)) {
        err = "framing: sequence field not found";
        return false;
    }
    if (cfg_.has_count && !bind(cfg_.packet_header, cfg_.count_field, count_)) {
        err = "framing: count field not found";
        return false;
    }
    if (cfg_.len_source == LenSource::MESSAGE_PREFIX && !bind(cfg_.message_prefix, cfg_.len_field, len_)) {
        err = "framing: length field not found";
        return false;
    }
    return true;
}

uint64_t Framer::read(const uint8_t* p, const Accessor& a) noexcept {
    const uint8_t* q = p + a.offset;
    switch (a.size) {
        case 1: return q[0];
        case 2: { uint16_t v; std::memcpy(&v, q, 2); return a.big ? __builtin_bswap16(v) : v; }
        case 4: { uint32_t v; std::memcpy(&v, q, 4); return a.big ? __builtin_bswap32(v) : v; }
        case 8: { uint64_t v; std::memcpy(&v, q, 8); return a.big ? __builtin_bswap64(v) : v; }
        default: return 0;
    }
}

bool Framer::parse(const PacketView& pkt, FrameInfo& info, MessageIter& it) const noexcept {
    info = FrameInfo{};
    it = MessageIter{};
    if (pkt.len < cfg_.packet_header_size) {
        return false;
    }
    if (seq_.present) {
        info.seq = read(pkt.data, seq_);
        info.has_seq = true;
    }
    if (count_.present) {
        info.count = static_cast<uint32_t>(read(pkt.data, count_));
        info.has_count = true;
        it.limit = info.count;
    }
    it.cur = pkt.data + cfg_.packet_header_size;
    it.end = pkt.data + pkt.len;
    return true;
}

NextStatus Framer::next(MessageIter& it, const uint8_t*& msg, uint32_t& len) const noexcept {
    if (it.cur >= it.end || (it.limit != 0 && it.index >= it.limit)) {
        return NextStatus::END;
    }
    const auto avail = static_cast<uint32_t>(it.end - it.cur);
    if (avail < cfg_.message_prefix_size) {
        return NextStatus::TRUNCATED;
    }
    msg = it.cur + cfg_.message_prefix_size;
    const uint32_t body_avail = avail - cfg_.message_prefix_size;

    if (cfg_.len_source == LenSource::PACKET) {
        len = body_avail; // one message per packet; the decoder validates its own length
    } else if (len_.present) {
        uint64_t declared = read(it.cur, len_);
        if (cfg_.length_includes_prefix) {
            if (declared < cfg_.message_prefix_size) return NextStatus::MALFORMED;
            declared -= cfg_.message_prefix_size;
        }
        if (declared == 0 || declared > body_avail) {
            return NextStatus::TRUNCATED;
        }
        len = static_cast<uint32_t>(declared);
    } else {
        // INTENT: no prefix on this bus. The schema fully describes message
        // length, so walk it. An unknown template cannot be skipped without a
        // prefix: the rest of the packet is undecodable and must be dropped
        // (never guessed), and the operator sees it in the counters.
        uint32_t total = 0;
        switch (walker_->walk(msg, body_avail, total)) {
            case WalkStatus::OK: break;
            case WalkStatus::UNKNOWN_TEMPLATE: return NextStatus::UNDECODABLE;
            case WalkStatus::TRUNCATED: return NextStatus::TRUNCATED;
            case WalkStatus::MALFORMED: return NextStatus::MALFORMED;
        }
        len = total;
    }
    it.cur = msg + len;
    ++it.index;
    if (cfg_.keep_prefix) {
        msg -= cfg_.message_prefix_size;
        len += cfg_.message_prefix_size;
    }
    return NextStatus::OK;
}

} // namespace subplane
