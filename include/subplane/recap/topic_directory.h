#pragma once

// TopicDirectory: a topic's instruments as its resolver last listed them (SPP
// ResolveReply): each instrument's id, venue, home partition and venue symbol.
// Written by the resolver client on the recap manager's thread, read by the
// session manager to turn ticker names into ids. Cold on both sides, so a mutex.

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace subplane {

struct ResolvedInstrument {
    uint64_t    instrument = 0;     // as on the wire
    uint16_t    venue = 0;
    uint16_t    partition_num = 0;  // its home
    std::string symbol;             // the venue symbol
};

class TopicDirectory {
public:
    TopicDirectory(std::string topic_name, uint16_t topic_id) : topic_name_(std::move(topic_name)), topic_id_(topic_id) {}

    const std::string& topic_name() const noexcept { return topic_name_; }
    uint16_t topic_id() const noexcept { return topic_id_; }

    // Replaces the list at once (one complete answer).
    void publish(std::vector<ResolvedInstrument> list);
    // 0 until the first list; +1 per list.
    uint64_t version() const noexcept { return version_.load(std::memory_order_acquire); }
    // By venue symbol, exactly as spelled.
    bool find(std::string_view symbol, ResolvedInstrument& out) const;
    size_t size() const;

private:
    std::string topic_name_;
    uint16_t topic_id_;
    mutable std::mutex mu_;
    std::unordered_map<std::string, ResolvedInstrument> by_symbol_;
    std::atomic<uint64_t> version_{0};
};

} // namespace subplane
