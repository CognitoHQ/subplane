#include "subplane/decoder/sbe_routing_decoder.h"

#include <string_view>

namespace subplane {
namespace {

struct NameCode { const char* name; uint8_t code; };
constexpr NameCode kMsgTypes[] = {
    {"TRADE", static_cast<uint8_t>(MsgType::TRADE)},
    {"QUOTE", static_cast<uint8_t>(MsgType::QUOTE)},
    {"BOOK_UPDATE", static_cast<uint8_t>(MsgType::BOOK_UPDATE)},
    {"BOOK_SNAPSHOT", static_cast<uint8_t>(MsgType::BOOK_SNAPSHOT)},
    {"OHLCV", static_cast<uint8_t>(MsgType::OHLCV)},
    {"STATUS", static_cast<uint8_t>(MsgType::STATUS)},
};
constexpr NameCode kAdmin[] = {
    {"HEARTBEAT", static_cast<uint8_t>(ControlCode::BUS_HEARTBEAT)},
    {"SEQ_RESET", static_cast<uint8_t>(ControlCode::BUS_SEQ_RESET)},
    {"SNAPSHOT_START", static_cast<uint8_t>(ControlCode::BUS_SNAPSHOT_START)},
    {"SNAPSHOT_END", static_cast<uint8_t>(ControlCode::BUS_SNAPSHOT_END)},
    {"TRADING_STATUS", static_cast<uint8_t>(ControlCode::BUS_TRADING_STATUS)},
};

template <size_t N>
bool lookup(const NameCode (&tbl)[N], std::string_view n, uint8_t& code) {
    for (const auto& e : tbl) if (n == e.name) { code = e.code; return true; }
    return false;
}

bool is_int_prim(SbePrim p) {
    switch (p) {
        case SbePrim::U8: case SbePrim::U16: case SbePrim::U32: case SbePrim::U64:
        case SbePrim::I8: case SbePrim::I16: case SbePrim::I32: case SbePrim::I64: return true;
        default: return false;
    }
}

} // namespace

bool parse_msg_type_name(std::string_view name, uint8_t& code) { return lookup(kMsgTypes, name, code); }
bool parse_admin_name(std::string_view name, uint8_t& code) { return lookup(kAdmin, name, code); }
bool parse_symbol_id_mode(std::string_view name, SymbolIdMode& out) {
    if (name == "table") { out = SymbolIdMode::TABLE; return true; }
    if (name == "passthrough") { out = SymbolIdMode::PASSTHROUGH; return true; }
    return false;
}

bool parse_decoder_config(const YAML::Node& node, DecoderConfig& out, std::string& err) {
    try {
        out = DecoderConfig{};
        if (!node || !node.IsMap()) { err = "decoder: expected a map"; return false; }
        if (node["type"] && node["type"].as<std::string>() != "sbe_routing") {
            err = "decoder.type '" + node["type"].as<std::string>() + "' is not supported (only sbe_routing)";
            return false;
        }
        if (!node["schema"]) { err = "decoder.schema is required"; return false; }
        out.schema_path = node["schema"].as<std::string>();
        if (const auto f = node["fields"]) {
            if (!f.IsMap()) { err = "decoder.fields must be a map"; return false; }
            for (const auto& kv : f) {
                const std::string k = kv.first.as<std::string>();
                const std::string v = kv.second.as<std::string>();
                if (k == "symbol") out.fields.symbol = v;
                else if (k == "sequence") out.fields.sequence = v;
                else if (k == "exchange_ts") out.fields.exchange_ts = v;
                else if (k == "venue") out.fields.venue = v;
                else { err = "decoder.fields: unknown routing field '" + k + "'"; return false; }
            }
        }
        const std::string mode = node["symbol_id_mode"] ? node["symbol_id_mode"].as<std::string>() : "table";
        if (!parse_symbol_id_mode(mode, out.symbol_mode)) { err = "decoder.symbol_id_mode must be table or passthrough, got '" + mode + "'"; return false; }

        const auto msgs = node["messages"];
        if (!msgs || !msgs.IsSequence() || msgs.size() == 0) { err = "decoder.messages must be a non-empty list"; return false; }
        for (const auto& m : msgs) {
            TemplateMapping tm;
            if (!m["template"]) { err = "decoder.messages entry lacks 'template'"; return false; }
            const long t = m["template"].as<long>();
            if (t < 0 || t > 65535) { err = "decoder.messages template out of range: " + std::to_string(t); return false; }
            tm.template_id = static_cast<uint16_t>(t);
            for (const auto& o : out.messages)
                if (o.template_id == tm.template_id) { err = "decoder.messages: duplicate template " + std::to_string(t); return false; }
            const bool has_mt = static_cast<bool>(m["msg_type"]), has_ad = static_cast<bool>(m["admin"]);
            if (has_mt == has_ad) { err = "decoder.messages template " + std::to_string(t) + ": exactly one of msg_type / admin required"; return false; }
            if (has_mt) {
                const std::string n = m["msg_type"].as<std::string>();
                if (!lookup(kMsgTypes, n, tm.msg_type)) { err = "decoder.messages: unknown msg_type '" + n + "'"; return false; }
            } else {
                const std::string n = m["admin"].as<std::string>();
                tm.is_admin = true;
                if (!lookup(kAdmin, n, tm.admin_code)) { err = "decoder.messages: unknown admin '" + n + "'"; return false; }
            }
            out.messages.push_back(tm);
        }
        return true;
    } catch (const YAML::Exception& e) {
        err = std::string("decoder config: ") + e.what();
        return false;
    }
}

bool SbeRoutingDecoder::init(const DecoderConfig& cfg, const SbeSchema& schema, std::string& err) {
    warnings_.clear();
    by_template_.clear();
    if (!walker_.build(schema, err)) return false;
    hdr_ = schema.header();
    little_ = schema.little_endian();
    schema_id_ = schema.schema_id();
    passthrough_ = cfg.symbol_mode == SymbolIdMode::PASSTHROUGH;

    uint16_t max_id = 0;
    for (const auto& tm : cfg.messages) if (tm.template_id > max_id) max_id = tm.template_id;
    by_template_.resize(static_cast<size_t>(max_id) + 1);

    for (const auto& tm : cfg.messages) {
        const MessageDef* md = schema.find_message(tm.template_id);
        if (!md) { err = "template " + std::to_string(tm.template_id) + " is not in the schema"; return false; }
        Entry& e = by_template_[tm.template_id];
        e.present = true;
        e.admin = tm.is_admin;
        e.msg_type = tm.is_admin ? tm.admin_code : tm.msg_type;
        e.prog = walker_.program(tm.template_id);

        struct Map { const std::string& name; const char* role; FieldRef* ref; };
        const Map maps[] = {{cfg.fields.symbol, "symbol", &e.symbol},
                            {cfg.fields.sequence, "sequence", &e.sequence},
                            {cfg.fields.exchange_ts, "exchange_ts", &e.exchange_ts},
                            {cfg.fields.venue, "venue", &e.venue}};
        for (const auto& m : maps) {
            if (m.name.empty()) continue;
            const FieldDef* f = schema.find_field(*md, m.name);
            if (!f) {
                if (schema.is_nested_field(*md, m.name)) {
                    err = "field " + m.name + " of message " + md->name +
                          " is inside a group/data; only root-block fields can be routing fields";
                    return false;
                }
                if (!tm.is_admin)
                    warnings_.push_back("message " + md->name + " has no field " + m.name + " (" + m.role + " left 0)");
                continue;
            }
            const bool is_symbol = m.ref == &e.symbol;
            if (is_symbol && f->prim == SbePrim::CHAR_ARRAY) {
                if (passthrough_) { err = "symbol_id_mode passthrough requires an integer symbol field; " + m.name + " of message " + md->name + " is a char array"; return false; }
                m.ref->is_str = true;
            } else if (!is_int_prim(f->prim)) {
                err = std::string(m.role) + " field " + m.name + " of message " + md->name + " must be an integer type";
                return false;
            }
            m.ref->offset = f->offset;
            m.ref->size = static_cast<uint8_t>(f->size);
            m.ref->end = static_cast<uint16_t>(f->offset + f->size);
        }
    }
    return true;
}

DecodeResult SbeRoutingDecoder::decode(const uint8_t* msg, uint32_t avail, TickHeader& out,
                                       SymbolResolver& syms) const noexcept {
    DecodeResult r;
    if (avail < hdr_.size) { r.status = DecodeStatus::TRUNCATED; return r; }
    using detail::sbe_read_uint;
    const uint64_t sid = sbe_read_uint(msg + hdr_.schema_id_off, hdr_.schema_id_size, little_);
    if (sid != schema_id_) { r.status = DecodeStatus::SCHEMA_MISMATCH; return r; }
    const uint64_t tid = sbe_read_uint(msg + hdr_.template_id_off, hdr_.template_id_size, little_);
    out.template_id = static_cast<uint16_t>(tid > 0xFFFF ? 0xFFFF : tid); // visible to last_unknown_template
    if (tid >= by_template_.size() || !by_template_[tid].present) {
        r.status = DecodeStatus::UNKNOWN_TEMPLATE;
        return r;
    }
    const Entry& e = by_template_[tid];
    const uint64_t blk = sbe_read_uint(msg + hdr_.block_length_off, hdr_.block_length_size, little_);
    const uint64_t root_end = static_cast<uint64_t>(hdr_.size) + blk;
    if (root_end > avail) { r.status = DecodeStatus::TRUNCATED; return r; }

    const uint64_t ver = sbe_read_uint(msg + hdr_.version_off, hdr_.version_size, little_);
    const WalkStatus ws = walker_.walk_program(*e.prog, msg, avail, static_cast<uint32_t>(root_end),
                                               static_cast<uint16_t>(ver > 0xFFFF ? 0xFFFF : ver), r.total_len);
    if (ws != WalkStatus::OK) {
        r.status = ws == WalkStatus::TRUNCATED ? DecodeStatus::TRUNCATED : DecodeStatus::MALFORMED;
        r.total_len = 0;
        return r;
    }

    const uint8_t* root = msg + hdr_.size;
    // INTENT: a field beyond the actual root block (older publisher version with a
    // smaller blockLength) is treated as absent, never read past the block.
    auto field_ok = [&](const FieldRef& f) noexcept { return f.end != 0 && f.end <= blk; };

    out.template_id = static_cast<uint16_t>(tid);
    out.schema_id = static_cast<uint16_t>(sid);
    out.schema_version = static_cast<uint16_t>(sbe_read_uint(msg + hdr_.version_off, hdr_.version_size, little_));
    out.wire_format = static_cast<uint8_t>(WireFormat::SBE);
    out.kind = static_cast<uint8_t>(e.admin ? Kind::BUS_ADMIN : Kind::DATA);
    out.msg_type = e.msg_type;
    out.sequence = field_ok(e.sequence) ? sbe_read_uint(root + e.sequence.offset, e.sequence.size, little_) : 0;
    out.exchange_ts = field_ok(e.exchange_ts) ? sbe_read_uint(root + e.exchange_ts.offset, e.exchange_ts.size, little_) : 0;
    out.venue_id = field_ok(e.venue) ? static_cast<uint16_t>(sbe_read_uint(root + e.venue.offset, e.venue.size, little_)) : 0;
    out.symbol_id = 0;
    if (field_ok(e.symbol)) {
        if (e.symbol.is_str) {
            const char* s = reinterpret_cast<const char*>(root + e.symbol.offset);
            uint32_t n = e.symbol.size;
            while (n > 0 && (s[n - 1] == '\0' || s[n - 1] == ' ')) --n;
            out.symbol_id = syms.resolve_str(s, n);
        } else {
            const uint64_t raw = sbe_read_uint(root + e.symbol.offset, e.symbol.size, little_);
            out.symbol_id = passthrough_ ? static_cast<uint32_t>(raw) : syms.resolve_u64(raw);
        }
    }
    r.status = DecodeStatus::OK;
    return r;
}

} // namespace subplane
