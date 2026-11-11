#pragma once

// Topics (architecture.md, "Topics and partitions"). A topic is a named feed
// holding partitions numbered from 1; each partition is one stream with one
// publisher and one sequence. Apps see topics, never partitions.
//
//   topic_name     string [A-Za-z0-9._-], 1..48 chars (Kafka-legal), site policy
//   topic_id       u16 1..32767, assigned once by the site in the registry file
//   partition_num  u16 1..65535, from the publisher's config
//   stream_id      i32 = topic_id << 16 | partition_num, always > 0
//
// Cold path only.

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <yaml-cpp/yaml.h>

namespace subplane {

inline constexpr uint32_t kTopicNameMax  = 48;
inline constexpr uint16_t kTopicIdMax    = 32767;

// INTENT: topic_id keeps its top bit clear so stream_id is a positive int32,
// which is what Aeron's stream id is; a site can then use stream_id as the
// Aeron stream id without a table.
inline constexpr int32_t stream_id_of(uint16_t topic_id, uint16_t partition_num) noexcept {
    return static_cast<int32_t>((static_cast<uint32_t>(topic_id) << 16) | partition_num);
}

// False (with err) unless `name` is 1..48 chars of [A-Za-z0-9._-] with no
// empty `.` segment.
bool valid_topic_name(std::string_view name, std::string& err);

// Pattern match on `.` segments: `*` matches exactly one segment, a final `>`
// matches one or more; anything else matches itself.
bool topic_matches(std::string_view pattern, std::string_view name);
// False (with err) unless `pattern` is a topic name with `*` segments and an
// optional final `>`.
bool valid_topic_pattern(std::string_view pattern, std::string& err);

struct TopicEntry {
    uint16_t    id = 0;
    std::string name;
};

// The site's topic registry: topic_name <-> topic_id. Agent and gateway read
// the same file (`site/topics.yaml`), so an id means the same on every box.
class TopicRegistry {
public:
    // A sequence of {id, name}. Errors: bad name, id outside 1..32767,
    // duplicate id or name.
    bool load(const YAML::Node& node, const std::string& where, std::string& err);
    bool load_file(const std::string& path, std::string& err);

    const TopicEntry* find(std::string_view name) const noexcept;
    const TopicEntry* find(uint16_t id) const noexcept;
    const std::vector<TopicEntry>& entries() const noexcept { return entries_; }

private:
    std::vector<TopicEntry> entries_;
};

} // namespace subplane
