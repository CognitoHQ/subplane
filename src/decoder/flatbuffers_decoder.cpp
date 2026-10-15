#include "subplane/decoder/flatbuffers_decoder.h"

#include "subplane/decoder/flatbuffers_wire.h"

#include <cstring>

namespace subplane {
namespace {

struct ScalarName { const char* name; FbScalar type; };
constexpr ScalarName kScalars[] = {
    {"u8", FbScalar::U8}, {"u16", FbScalar::U16}, {"u32", FbScalar::U32}, {"u64", FbScalar::U64},
    {"i8", FbScalar::I8}, {"i16", FbScalar::I16}, {"i32", FbScalar::I32}, {"i64", FbScalar::I64},
    {"string", FbScalar::STRING},
};

bool parse_path(const YAML::Node& n, const std::string& what, bool allow_string, FbFieldPath& out, std::string& err) {
    out = FbFieldPath{};
    if (!n.IsMap() || !n["path"] || !n["type"]) { err = what + ": expected {path: [ids...], type: <scalar>}"; return false; }
    const auto p = n["path"];
    if (!p.IsSequence() || p.size() == 0 || p.size() > FbFieldPath::kMaxDepth) {
        err = what + ".path must list 1.." + std::to_string(FbFieldPath::kMaxDepth) + " field ids"; return false;
    }
    for (size_t i = 0; i < p.size(); ++i) {
        const long v = p[i].as<long>();
        if (v < 0 || v > 65535) { err = what + ".path: field id out of range"; return false; }
        out.ids[i] = static_cast<uint16_t>(v);
    }
    out.depth = static_cast<uint8_t>(p.size());
    const std::string t = n["type"].as<std::string>();
    if (!parse_fb_scalar(t, out.type)) { err = what + ".type: unknown scalar '" + t + "'"; return false; }
    if (out.type == FbScalar::STRING && !allow_string) { err = what + ".type: string is only valid for symbol in table mode"; return false; }
    return true;
}

// Reads an integer at a path: all ids but the last are table (or union value) fields.
inline bool read_int(fbw::Buf s, uint32_t tbl, const FbFieldPath& f, uint64_t& v) noexcept {
    for (uint8_t i = 0; i + 1 < f.depth; ++i) {
        tbl = fbw::deref(s, tbl, f.ids[i]);
        if (tbl == 0) return false;
    }
    const uint32_t at = fbw::field(s, tbl, f.ids[f.depth - 1]);
    if (at == 0) return false;
    switch (f.type) {
        case FbScalar::U8:  { uint8_t x;  if (!fbw::rd(s, at, x)) return false; v = x; return true; }
        case FbScalar::U16: { uint16_t x; if (!fbw::rd(s, at, x)) return false; v = x; return true; }
        case FbScalar::U32: { uint32_t x; if (!fbw::rd(s, at, x)) return false; v = x; return true; }
        case FbScalar::U64: { uint64_t x; if (!fbw::rd(s, at, x)) return false; v = x; return true; }
        case FbScalar::I8:  { int8_t x;   if (!fbw::rd(s, at, x)) return false; v = static_cast<uint64_t>(static_cast<int64_t>(x)); return true; }
        case FbScalar::I16: { int16_t x;  if (!fbw::rd(s, at, x)) return false; v = static_cast<uint64_t>(static_cast<int64_t>(x)); return true; }
        case FbScalar::I32: { int32_t x;  if (!fbw::rd(s, at, x)) return false; v = static_cast<uint64_t>(static_cast<int64_t>(x)); return true; }
        case FbScalar::I64: { int64_t x;  if (!fbw::rd(s, at, x)) return false; v = static_cast<uint64_t>(x); return true; }
        default: return false;
    }
}

inline bool read_str(fbw::Buf s, uint32_t tbl, const FbFieldPath& f, const char*& p, uint32_t& len) noexcept {
    for (uint8_t i = 0; i + 1 < f.depth; ++i) {
        tbl = fbw::deref(s, tbl, f.ids[i]);
        if (tbl == 0) return false;
    }
    return fbw::string(s, tbl, f.ids[f.depth - 1], p, len);
}

} // namespace

bool parse_fb_scalar(std::string_view name, FbScalar& out) {
    for (const auto& s : kScalars) if (name == s.name) { out = s.type; return true; }
    return false;
}

bool parse_flatbuffers_decoder_config(const YAML::Node& node, FlatbuffersDecoderConfig& out, std::string& err) {
    try {
        out = FlatbuffersDecoderConfig{};
        if (!node || !node.IsMap()) { err = "decoder: expected a map"; return false; }
        if (node["schema"]) { err = "decoder.schema is not used by type flatbuffers (fields are named by id)"; return false; }
        if (node["fields"]) { err = "decoder.fields is not used by type flatbuffers (routing fields are per message)"; return false; }
        out.size_prefixed = node["size_prefixed"].as<bool>(true);
        const std::string mode = node["symbol_id_mode"] ? node["symbol_id_mode"].as<std::string>() : "passthrough";
        if (!parse_symbol_id_mode(mode, out.symbol_mode)) { err = "decoder.symbol_id_mode must be table or passthrough, got '" + mode + "'"; return false; }
        const bool table = out.symbol_mode == SymbolIdMode::TABLE;
        const std::string scope = node["symbol_scope"] ? node["symbol_scope"].as<std::string>() : "global";
        if (!parse_symbol_scope(scope, out.venue_scope)) { err = "decoder.symbol_scope must be global or venue, got '" + scope + "'"; return false; }
        if (out.venue_scope && !table) { err = "decoder.symbol_scope: venue requires symbol_id_mode: table"; return false; }

        const auto d = node["dispatch"];
        if (!d) { err = "decoder.dispatch is required: identifier, or {type_field: {path, type}}"; return false; }
        if (d.IsScalar()) {
            if (d.as<std::string>() != "identifier") { err = "decoder.dispatch must be identifier or {type_field: {path, type}}"; return false; }
            out.dispatch = FbDispatch::IDENTIFIER;
        } else if (d.IsMap() && d["type_field"]) {
            out.dispatch = FbDispatch::TYPE_FIELD;
            if (!parse_path(d["type_field"], "decoder.dispatch.type_field", false, out.type_field, err)) return false;
            if (out.type_field.type != FbScalar::U8 && out.type_field.type != FbScalar::U16 && out.type_field.type != FbScalar::U32) {
                err = "decoder.dispatch.type_field.type must be u8, u16 or u32"; return false;
            }
        } else {
            err = "decoder.dispatch must be identifier or {type_field: {path, type}}"; return false;
        }

        const auto msgs = node["messages"];
        if (!msgs || !msgs.IsSequence() || msgs.size() == 0) { err = "decoder.messages must be a non-empty list"; return false; }
        if (msgs.size() > FlatbuffersDecoder::kMaxMessages) { err = "decoder.messages: too many entries"; return false; }
        for (const auto& m : msgs) {
            FbMessageSpec spec;
            std::string key;
            if (out.dispatch == FbDispatch::TYPE_FIELD) {
                if (!m["type"]) { err = "decoder.messages entry lacks 'type' (type_field dispatch)"; return false; }
                if (m["identifier"]) { err = "decoder.messages: 'identifier' is only valid with dispatch: identifier"; return false; }
                const long v = m["type"].as<long>();
                if (v < 0 || v > 65535) { err = "decoder.messages: type out of range 0..65535"; return false; }
                spec.type = static_cast<uint32_t>(v);
                key = "type " + std::to_string(v);
                for (const auto& o : out.messages) if (o.type == spec.type) { err = "decoder.messages: duplicate " + key; return false; }
            } else {
                if (!m["identifier"]) { err = "decoder.messages entry lacks 'identifier' (identifier dispatch)"; return false; }
                if (m["type"]) { err = "decoder.messages: 'type' is only valid with type_field dispatch"; return false; }
                const std::string id = m["identifier"].as<std::string>();
                if (id.size() != 4) { err = "decoder.messages: identifier '" + id + "' must be exactly 4 characters"; return false; }
                std::memcpy(spec.identifier, id.data(), 4);
                key = "identifier " + id;
                for (const auto& o : out.messages) if (std::memcmp(o.identifier, spec.identifier, 4) == 0) { err = "decoder.messages: duplicate " + key; return false; }
            }
            const bool has_mt = static_cast<bool>(m["msg_type"]), has_ad = static_cast<bool>(m["admin"]);
            if (has_mt == has_ad) { err = "decoder.messages " + key + ": exactly one of msg_type / admin required"; return false; }
            if (has_mt) {
                const std::string n = m["msg_type"].as<std::string>();
                if (!parse_msg_type_name(n, spec.msg_type)) { err = "decoder.messages: unknown msg_type '" + n + "'"; return false; }
            } else {
                const std::string n = m["admin"].as<std::string>();
                spec.is_admin = true;
                if (!parse_recap_marker_name(n, spec.marker) && !parse_admin_name(n, spec.msg_type)) {
                    err = "decoder.messages: unknown admin '" + n + "'"; return false;
                }
            }
            const std::string pre = "decoder.messages " + key + " ";
            if (const auto rf = m["fields"]) {
                if (spec.marker == RecapMarker::NONE) { err = pre + "'fields' is only valid on RECAP_BEGIN, RECAP_END and RECAP_REJECT"; return false; }
                if (!rf.IsMap()) { err = pre + "fields must be a map"; return false; }
                bool has[kRecapFields] = {};
                for (const auto& kv : rf) {
                    const std::string k = kv.first.as<std::string>();
                    RecapField f;
                    if (!parse_recap_field_name(k, f)) { err = pre + "fields: unknown recap field '" + k + "' (request_id, as_of, source_epoch, reason)"; return false; }
                    if (!parse_path(kv.second, pre + "fields." + k, false, spec.recap[f], err)) return false;
                    has[f] = true;
                }
                if (const char* miss = recap_missing_field(spec.marker, has)) { err = pre + "fields: '" + miss + "' is required"; return false; }
            } else if (spec.marker != RecapMarker::NONE) {
                err = pre + "a recap marker needs 'fields' (at least request_id)"; return false;
            }
            if (m["symbol"]      && !parse_path(m["symbol"],      pre + "symbol",      table, spec.symbol,      err)) return false;
            if (m["venue"]       && !parse_path(m["venue"],       pre + "venue",       false, spec.venue,       err)) return false;
            if (m["exchange_ts"] && !parse_path(m["exchange_ts"], pre + "exchange_ts", false, spec.exchange_ts, err)) return false;
            if (m["sequence"]    && !parse_path(m["sequence"],    pre + "sequence",    false, spec.sequence,    err)) return false;
            if (out.venue_scope && spec.symbol.depth != 0) {
                if (spec.symbol.type == FbScalar::STRING) { err = pre + "symbol: symbol_scope: venue requires an integer symbol"; return false; }
                if (spec.venue.depth == 0) { err = pre + "lacks venue, which symbol_scope: venue requires"; return false; }
            }
            out.messages.push_back(spec);
        }
        return true;
    } catch (const YAML::Exception& e) {
        err = std::string("decoder config: ") + e.what();
        return false;
    }
}

bool FlatbuffersDecoder::init(const FlatbuffersDecoderConfig& cfg, std::string& err) {
    warnings_.clear();
    entries_.clear();
    for (auto& x : by_u8_) x = -1;
    prefix_ = cfg.size_prefixed ? 4 : 0;
    passthrough_ = cfg.symbol_mode == SymbolIdMode::PASSTHROUGH;
    venue_scope_ = cfg.venue_scope;
    dispatch_ = cfg.dispatch;
    type_field_ = cfg.type_field;
    if (cfg.messages.empty()) { err = "flatbuffers decoder: no message mappings"; return false; }
    if (cfg.messages.size() > kMaxMessages) { err = "flatbuffers decoder: too many messages"; return false; }
    entries_.reserve(cfg.messages.size());
    for (size_t i = 0; i < cfg.messages.size(); ++i) {
        const FbMessageSpec& m = cfg.messages[i];
        Entry e;
        e.present = true;
        e.admin = m.is_admin;
        e.msg_type = m.msg_type;
        e.marker = m.marker;
        for (uint8_t f = 0; f < kRecapFields; ++f) e.recap[f] = m.recap[f];
        std::memcpy(e.identifier, m.identifier, 4);
        e.symbol = m.symbol; e.venue = m.venue; e.exchange_ts = m.exchange_ts; e.sequence = m.sequence;
        // INTENT: template_id is the dispatch value for type_field dispatch and
        // the 1-based position in `messages` for identifier dispatch, so that
        // last_unknown_template and the record header carry something stable.
        e.template_id = dispatch_ == FbDispatch::TYPE_FIELD ? static_cast<uint16_t>(m.type) : static_cast<uint16_t>(i + 1);
        if (dispatch_ == FbDispatch::TYPE_FIELD && type_field_.type == FbScalar::U8 && m.type < 256)
            by_u8_[m.type] = static_cast<int16_t>(i);
        if (m.symbol.depth == 0 && m.marker == RecapMarker::NONE) warnings_.push_back("message " + std::to_string(e.template_id) + " has no symbol path; routed by type only");
        entries_.push_back(e);
    }
    return true;
}

const FlatbuffersDecoder::Entry* FlatbuffersDecoder::find(const uint8_t* msg, uint32_t avail, uint32_t root,
                                                          uint16_t& template_id) const noexcept {
    const fbw::Buf s{msg, avail};
    if (dispatch_ == FbDispatch::IDENTIFIER) {
        char id[4];
        if (!fbw::file_id(s, prefix_, id)) return nullptr;
        for (size_t i = 0; i < entries_.size(); ++i) {
            if (std::memcmp(entries_[i].identifier, id, 4) == 0) { template_id = entries_[i].template_id; return &entries_[i]; }
        }
        template_id = 0;
        return nullptr;
    }
    uint64_t t;
    if (!read_int(s, root, type_field_, t)) { template_id = 0; return nullptr; }
    template_id = static_cast<uint16_t>(t);
    if (type_field_.type == FbScalar::U8) {
        const int16_t i = by_u8_[t & 0xFF];
        return i >= 0 ? &entries_[static_cast<size_t>(i)] : nullptr;
    }
    for (const Entry& e : entries_) if (e.template_id == t) return &e;
    return nullptr;
}

DecodeResult FlatbuffersDecoder::decode(const uint8_t* msg, uint32_t avail, TickHeader& out,
                                        SymbolResolver& syms) const noexcept {
    DecodeResult r;
    if (avail < prefix_ + 8) { r.status = DecodeStatus::TRUNCATED; return r; }
    const fbw::Buf s{msg, avail};
    // INTENT: the framer hands one whole buffer. With a size prefix the prefix
    // must account for exactly the rest: a disagreement means the stream or
    // the framing is misaligned and must surface, never route garbage.
    if (prefix_ == 4) {
        uint32_t size;
        std::memcpy(&size, msg, 4);
        if (size > avail - 4) { r.status = DecodeStatus::TRUNCATED; return r; }
        if (size != avail - 4) { r.status = DecodeStatus::MALFORMED; return r; }
    }
    r.total_len = avail;
    uint32_t root;
    if (!fbw::root(s, prefix_, root)) { r.status = DecodeStatus::MALFORMED; r.total_len = 0; return r; }
    uint16_t template_id = 0;
    const Entry* e = find(msg, avail, root, template_id);
    out.template_id = template_id; // visible to last_unknown_template on the UNKNOWN path
    if (!e) { r.status = DecodeStatus::UNKNOWN_TEMPLATE; return r; }

    out.schema_id = 0;
    out.schema_version = 0;
    out.wire_format = static_cast<uint8_t>(WireFormat::FLATBUFFERS);
    out.kind = static_cast<uint8_t>(e->admin ? Kind::BUS_ADMIN : Kind::DATA);
    out.msg_type = e->msg_type;
    r.marker = e->marker;
    uint64_t v;
    // INTENT: venue before symbol; symbol_scope: venue interns with it.
    out.venue_id    = (e->venue.depth != 0 && read_int(s, root, e->venue, v)) ? static_cast<uint16_t>(v) : 0;
    out.symbol_id = 0;
    if (e->symbol.depth != 0) {
        if (e->symbol.type == FbScalar::STRING) {
            const char* p; uint32_t len;
            if (read_str(s, root, e->symbol, p, len)) out.symbol_id = syms.resolve_str(p, len);
        } else if (read_int(s, root, e->symbol, v)) {
            out.symbol_id = passthrough_ ? static_cast<uint32_t>(v)
                                         : syms.resolve_u64(v, venue_scope_ ? out.venue_id : uint16_t{0});
        }
    }
    out.exchange_ts = (e->exchange_ts.depth != 0 && read_int(s, root, e->exchange_ts, v)) ? v : 0;
    out.sequence    = (e->sequence.depth != 0 && read_int(s, root, e->sequence, v)) ? v : 0;
    r.status = DecodeStatus::OK;
    return r;
}

bool FlatbuffersDecoder::read_recap_fields(const uint8_t* msg, uint32_t avail, RecapMarkerFields& out) const noexcept {
    out = RecapMarkerFields{};
    if (avail < prefix_ + 8) return false;
    const fbw::Buf s{msg, avail};
    uint32_t root;
    if (!fbw::root(s, prefix_, root)) return false;
    uint16_t template_id = 0;
    const Entry* e = find(msg, avail, root, template_id);
    if (!e || e->marker == RecapMarker::NONE) return false;
    for (uint8_t i = 0; i < kRecapFields; ++i) {
        if (e->recap[i].depth == 0) continue;
        uint64_t v;
        if (read_int(s, root, e->recap[i], v)) { out.value[i] = v; out.has[i] = true; }
    }
    return true;
}

} // namespace subplane
