#include "flashtick/pipeline/session.h"

namespace flashtick {

namespace {
struct NameCode { const char* name; uint8_t code; };
constexpr NameCode kMsgTypes[] = {
    {"TRADE",         static_cast<uint8_t>(MsgType::TRADE)},
    {"QUOTE",         static_cast<uint8_t>(MsgType::QUOTE)},
    {"BOOK_UPDATE",   static_cast<uint8_t>(MsgType::BOOK_UPDATE)},
    {"BOOK_SNAPSHOT", static_cast<uint8_t>(MsgType::BOOK_SNAPSHOT)},
    {"OHLCV",         static_cast<uint8_t>(MsgType::OHLCV)},
    {"STATUS",        static_cast<uint8_t>(MsgType::STATUS)},
};
} // namespace

bool msg_type_from_name(std::string_view name, uint8_t& code) {
    for (const auto& e : kMsgTypes) {
        if (name == e.name) {
            code = e.code;
            return true;
        }
    }
    return false;
}

const char* msg_type_name(uint8_t code) {
    for (const auto& e : kMsgTypes) {
        if (code == e.code) return e.name;
    }
    return "UNKNOWN";
}

bool SessionSpec::parse(const YAML::Node& node, SessionSpec& out, std::string& err) {
    out = SessionSpec{};
    try {
        out.name = node["name"].as<std::string>("");
        if (out.name.empty() || out.name.size() > 31) {
            err = "session.name is required (1..31 chars)";
            return false;
        }
        for (char c : out.name) {
            if (!(isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '-')) {
                err = "session.name may only contain [A-Za-z0-9_-]: " + out.name;
                return false;
            }
        }
        out.owner = node["owner"].as<std::string>("config");
        if (node["ring"].IsDefined()) {
            const auto r = node["ring"];
            if (r["capacity_mb"].IsDefined()) {
                out.ring_capacity_bytes = r["capacity_mb"].as<uint64_t>() << 20;
            } else if (r["capacity_bytes"].IsDefined()) {
                out.ring_capacity_bytes = r["capacity_bytes"].as<uint64_t>();
            }
            out.max_payload = r["max_payload"].as<uint32_t>(out.max_payload);
        }
        if (node["partitions"].IsDefined()) {
            for (const auto& p : node["partitions"]) out.partitions.push_back(p.as<std::string>());
        }
        if (node["symbols"].IsDefined()) {
            for (const auto& s : node["symbols"]) {
                std::string v = s.as<std::string>();
                if (v == "*") out.all_symbols = true; else out.symbols.push_back(v);
            }
        } else {
            out.all_symbols = true;
        }
        if (node["msg_types"].IsDefined()) {
            for (const auto& m : node["msg_types"]) {
                std::string v = m.as<std::string>();
                uint8_t code;
                if (!msg_type_from_name(v, code)) {
                    err = "session " + out.name + ": unknown msg_type " + v;
                    return false;
                }
                out.msg_types.push_back(v);
            }
        }
        if (node["deliver"].IsDefined()) {
            out.bus_admin = node["deliver"]["bus_admin"].as<bool>(true);
            out.agent_control = node["deliver"]["agent_control"].as<bool>(true);
        }
    } catch (const YAML::Exception& e) {
        err = "session " + out.name + ": " + e.what();
        return false;
    }
    if ((out.ring_capacity_bytes & (out.ring_capacity_bytes - 1)) != 0 || out.ring_capacity_bytes < (64u << 10)) {
        err = "session " + out.name + ": ring capacity must be a power of two >= 64 KiB";
        return false;
    }
    if (out.max_payload == 0 || out.max_payload > out.ring_capacity_bytes / 4) {
        err = "session " + out.name + ": max_payload must be 1..capacity/4";
        return false;
    }
    return true;
}

} // namespace flashtick
