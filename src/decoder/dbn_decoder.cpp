#include "subplane/decoder/dbn_decoder.h"

#include <cstring>

namespace subplane {
namespace {

// Record types and the offset of their u32 `sequence` field (DBN v3 layouts,
// from databento-cpp record.hpp). 0 = no sequence in that record.
constexpr DbnRtypeInfo kRtypes[] = {
    {"MBP_0",          0x00, 44}, // trade
    {"TRADE",          0x00, 44}, // alias
    {"MBP_1",          0x01, 44},
    {"MBP_10",         0x0A, 44},
    {"STATUS",         0x12, 0},
    {"DEFINITION",     0x13, 0},
    {"IMBALANCE",      0x14, 0},
    {"ERROR",          0x15, 0},
    {"SYMBOL_MAPPING", 0x16, 0},
    {"SYSTEM",         0x17, 0},
    {"STATISTICS",     0x18, 0},
    {"OHLCV_1S",       0x20, 0},
    {"OHLCV_1M",       0x21, 0},
    {"OHLCV_1H",       0x22, 0},
    {"OHLCV_1D",       0x23, 0},
    {"MBO",            0xA0, 52},
    {"CMBP_1",         0xB1, 0},
    {"CBBO_1S",        0xC0, 0},
    {"CBBO_1M",        0xC1, 0},
    {"BBO_1S",         0xC3, 0},
    {"BBO_1M",         0xC4, 0},
};

constexpr uint16_t kMinRecord = 16;

inline uint16_t rd16(const uint8_t* p) noexcept { uint16_t v; std::memcpy(&v, p, 2); return v; }
inline uint32_t rd32(const uint8_t* p) noexcept { uint32_t v; std::memcpy(&v, p, 4); return v; }
inline uint64_t rd64(const uint8_t* p) noexcept { uint64_t v; std::memcpy(&v, p, 8); return v; }

DbnMapping data(const char* rtype, MsgType mt) {
    DbnMapping m; m.rtype = dbn_rtype_by_name(rtype)->code; m.msg_type = static_cast<uint8_t>(mt); return m;
}
DbnMapping admin(const char* rtype, ControlCode cc) {
    DbnMapping m; m.rtype = dbn_rtype_by_name(rtype)->code; m.is_admin = true; m.admin_code = static_cast<uint8_t>(cc); return m;
}

} // namespace

const DbnRtypeInfo* dbn_rtype_by_name(std::string_view name) {
    for (const auto& r : kRtypes) if (name == r.name) return &r;
    return nullptr;
}
const DbnRtypeInfo* dbn_rtype_by_code(uint8_t code) {
    for (const auto& r : kRtypes) if (code == r.code) return &r;
    return nullptr;
}

std::vector<DbnMapping> dbn_default_mappings() {
    return {
        data("MBO", MsgType::BOOK_UPDATE),
        data("MBP_0", MsgType::TRADE),
        data("MBP_1", MsgType::QUOTE),
        data("CMBP_1", MsgType::QUOTE),
        data("BBO_1S", MsgType::QUOTE),
        data("BBO_1M", MsgType::QUOTE),
        data("CBBO_1S", MsgType::QUOTE),
        data("CBBO_1M", MsgType::QUOTE),
        data("MBP_10", MsgType::BOOK_SNAPSHOT),
        data("OHLCV_1S", MsgType::OHLCV),
        data("OHLCV_1M", MsgType::OHLCV),
        data("OHLCV_1H", MsgType::OHLCV),
        data("OHLCV_1D", MsgType::OHLCV),
        data("STATUS", MsgType::STATUS),
        data("STATISTICS", MsgType::STATUS),
        data("IMBALANCE", MsgType::STATUS),
        admin("SYSTEM", ControlCode::BUS_HEARTBEAT),
        // DEFINITION, SYMBOL_MAPPING, ERROR: unmapped by default (counted as unknown_templates).
    };
}

bool parse_dbn_decoder_config(const YAML::Node& node, DbnDecoderConfig& out, std::string& err) {
    try {
        out = DbnDecoderConfig{};
        if (!node || !node.IsMap()) { err = "decoder: expected a map"; return false; }
        if (node["schema"]) { err = "decoder.schema is not used by type dbn (record layout is fixed)"; return false; }
        if (node["fields"]) { err = "decoder.fields is not used by type dbn (routing fields are fixed)"; return false; }
        const std::string mode = node["symbol_id_mode"] ? node["symbol_id_mode"].as<std::string>() : "passthrough";
        if (!parse_symbol_id_mode(mode, out.symbol_mode)) { err = "decoder.symbol_id_mode must be table or passthrough, got '" + mode + "'"; return false; }

        const auto msgs = node["messages"];
        if (!msgs) { out.messages = dbn_default_mappings(); return true; }
        if (!msgs.IsSequence() || msgs.size() == 0) { err = "decoder.messages must be a non-empty list (or omitted for defaults)"; return false; }
        for (const auto& m : msgs) {
            DbnMapping dm;
            if (!m["rtype"]) { err = "decoder.messages entry lacks 'rtype'"; return false; }
            const std::string rt = m["rtype"].as<std::string>();
            if (const DbnRtypeInfo* info = dbn_rtype_by_name(rt)) {
                dm.rtype = info->code;
            } else {
                char* end = nullptr;
                const long v = std::strtol(rt.c_str(), &end, 0);
                if (end == rt.c_str() || *end != '\0' || v < 0 || v > 255) { err = "decoder.messages: unknown rtype '" + rt + "'"; return false; }
                dm.rtype = static_cast<uint8_t>(v);
            }
            for (const auto& o : out.messages)
                if (o.rtype == dm.rtype) { err = "decoder.messages: duplicate rtype " + rt; return false; }
            const bool has_mt = static_cast<bool>(m["msg_type"]), has_ad = static_cast<bool>(m["admin"]);
            if (has_mt == has_ad) { err = "decoder.messages rtype " + rt + ": exactly one of msg_type / admin required"; return false; }
            if (has_mt) {
                const std::string n = m["msg_type"].as<std::string>();
                if (!parse_msg_type_name(n, dm.msg_type)) { err = "decoder.messages: unknown msg_type '" + n + "'"; return false; }
            } else {
                const std::string n = m["admin"].as<std::string>();
                dm.is_admin = true;
                if (!parse_admin_name(n, dm.admin_code)) { err = "decoder.messages: unknown admin '" + n + "'"; return false; }
            }
            out.messages.push_back(dm);
        }
        return true;
    } catch (const YAML::Exception& e) {
        err = std::string("decoder config: ") + e.what();
        return false;
    }
}

bool DbnDecoder::init(const DbnDecoderConfig& cfg, std::string& err) {
    warnings_.clear();
    for (auto& e : by_rtype_) e = Entry{};
    passthrough_ = cfg.symbol_mode == SymbolIdMode::PASSTHROUGH;
    if (cfg.messages.empty()) { err = "dbn decoder: no message mappings"; return false; }
    for (const auto& m : cfg.messages) {
        Entry& e = by_rtype_[m.rtype];
        e.present = true;
        e.admin = m.is_admin;
        e.msg_type = m.is_admin ? m.admin_code : m.msg_type;
        const DbnRtypeInfo* info = dbn_rtype_by_code(m.rtype);
        e.sequence_off = info ? info->sequence_off : 0;
        if (!info) warnings_.push_back("rtype " + std::to_string(m.rtype) + " is not a known DBN record type; sequence left 0");
    }
    return true;
}

DecodeResult DbnDecoder::decode(const uint8_t* msg, uint32_t avail, TickHeader& out,
                                SymbolResolver& syms) const noexcept {
    DecodeResult r;
    if (avail < kMinRecord) { r.status = DecodeStatus::TRUNCATED; return r; }
    const uint32_t total = static_cast<uint32_t>(msg[0]) * 4u;
    const uint8_t rtype = msg[1];
    out.template_id = rtype; // visible to last_unknown_template on the UNKNOWN path
    r.total_len = total;
    // INTENT: the framer hands one whole record; a length byte that disagrees
    // means the stream is misaligned, which must surface as an error rather
    // than route garbage. Never trust a length past avail.
    if (total < kMinRecord || total > avail) { r.status = DecodeStatus::TRUNCATED; r.total_len = 0; return r; }
    if (total != avail) { r.status = DecodeStatus::MALFORMED; r.total_len = 0; return r; }
    const Entry& e = by_rtype_[rtype];
    if (!e.present) { r.status = DecodeStatus::UNKNOWN_TEMPLATE; return r; }

    out.schema_id = 0;
    out.schema_version = 0;
    out.wire_format = static_cast<uint8_t>(WireFormat::DBN);
    out.kind = static_cast<uint8_t>(e.admin ? Kind::BUS_ADMIN : Kind::DATA);
    out.msg_type = e.msg_type;
    out.venue_id = rd16(msg + 2);
    const uint32_t iid = rd32(msg + 4);
    out.symbol_id = passthrough_ ? iid : syms.resolve_u64(iid);
    out.exchange_ts = rd64(msg + 8);
    out.sequence = (e.sequence_off != 0 && e.sequence_off + 4u <= total) ? rd32(msg + e.sequence_off) : 0;
    r.status = DecodeStatus::OK;
    return r;
}

} // namespace subplane
