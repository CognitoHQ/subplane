#include "subplane/pipeline/topic_registry.h"

#include <algorithm>

namespace subplane {

namespace {
bool name_char(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-';
}

// Splits on '.'; empty segments are kept so callers can reject them.
std::vector<std::string_view> segments(std::string_view s) {
    std::vector<std::string_view> out;
    size_t start = 0;
    for (size_t i = 0; i <= s.size(); ++i) {
        if (i == s.size() || s[i] == '.') {
            out.push_back(s.substr(start, i - start));
            start = i + 1;
        }
    }
    return out;
}
} // namespace

bool valid_topic_name(std::string_view name, std::string& err) {
    if (name.empty() || name.size() > kTopicNameMax) {
        err = "topic name '" + std::string(name) + "' must be 1.." + std::to_string(kTopicNameMax) + " chars";
        return false;
    }
    for (std::string_view seg : segments(name)) {
        if (seg.empty()) {
            err = "topic name '" + std::string(name) + "' has an empty '.' segment";
            return false;
        }
        for (char c : seg) {
            if (!name_char(c)) {
                err = "topic name '" + std::string(name) + "' may only contain [A-Za-z0-9._-]";
                return false;
            }
        }
    }
    return true;
}

bool valid_topic_pattern(std::string_view pattern, std::string& err) {
    if (pattern.empty() || pattern.size() > kTopicNameMax) {
        err = "topic pattern '" + std::string(pattern) + "' must be 1.." + std::to_string(kTopicNameMax) + " chars";
        return false;
    }
    const auto segs = segments(pattern);
    for (size_t i = 0; i < segs.size(); ++i) {
        const std::string_view seg = segs[i];
        if (seg == "*") continue;
        if (seg == ">") {
            if (i + 1 != segs.size()) {
                err = "topic pattern '" + std::string(pattern) + "': '>' must be the last segment";
                return false;
            }
            continue;
        }
        if (seg.empty()) {
            err = "topic pattern '" + std::string(pattern) + "' has an empty '.' segment";
            return false;
        }
        for (char c : seg) {
            if (!name_char(c)) {
                err = "topic pattern '" + std::string(pattern) + "': a segment is a name, '*' or a final '>'";
                return false;
            }
        }
    }
    return true;
}

bool topic_matches(std::string_view pattern, std::string_view name) {
    const auto ps = segments(pattern), ns = segments(name);
    for (size_t i = 0; i < ps.size(); ++i) {
        if (ps[i] == ">") return i + 1 == ps.size() && ns.size() > i;
        if (i >= ns.size()) return false;
        if (ps[i] != "*" && ps[i] != ns[i]) return false;
    }
    return ps.size() == ns.size();
}

bool TopicRegistry::load(const YAML::Node& node, const std::string& where, std::string& err) {
    entries_.clear();
    if (!node.IsSequence()) {
        err = where + ": the topic registry is a sequence of { id, name }";
        return false;
    }
    try {
        for (const auto& e : node) {
            TopicEntry t;
            t.name = e["name"].as<std::string>("");
            const int64_t id = e["id"].as<int64_t>(0);
            if (!valid_topic_name(t.name, err)) {
                err = where + ": " + err;
                return false;
            }
            if (id < 1 || id > kTopicIdMax) {
                err = where + ": topic " + t.name + ": id must be 1.." + std::to_string(kTopicIdMax) + " (0 is reserved)";
                return false;
            }
            t.id = static_cast<uint16_t>(id);
            for (const auto& x : entries_) {
                if (x.id == t.id) {
                    err = where + ": topic id " + std::to_string(t.id) + " is given to both " + x.name + " and " + t.name;
                    return false;
                }
                if (x.name == t.name) {
                    err = where + ": topic " + t.name + " is listed twice";
                    return false;
                }
            }
            entries_.push_back(std::move(t));
        }
    } catch (const YAML::Exception& e) {
        err = where + ": " + e.what();
        return false;
    }
    return true;
}

bool TopicRegistry::load_file(const std::string& path, std::string& err) {
    YAML::Node n;
    try {
        n = YAML::LoadFile(path);
    } catch (const YAML::Exception& e) {
        err = "topic registry " + path + ": " + e.what();
        return false;
    }
    return load(n, "topic registry " + path, err);
}

const TopicEntry* TopicRegistry::find(std::string_view name) const noexcept {
    for (const auto& e : entries_) if (e.name == name) return &e;
    return nullptr;
}

const TopicEntry* TopicRegistry::find(uint16_t id) const noexcept {
    for (const auto& e : entries_) if (e.id == id) return &e;
    return nullptr;
}

} // namespace subplane
