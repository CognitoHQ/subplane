#include <doctest.h>

#include "subplane/core/filter_table.h"

using namespace subplane;

namespace {
TickHeader hdr(Kind k, uint8_t type, uint32_t sym, uint16_t topic, uint16_t venue = 1) {
    TickHeader h{};
    h.kind = static_cast<uint8_t>(k);
    h.msg_type = type;
    h.symbol_id = sym;
    h.topic_id = topic;
    h.venue_id = venue;
    return h;
}
std::unique_ptr<FilterTable> make(const FilterSpec& s, uint32_t max = 1000) {
    std::string err;
    auto t = build_filter(s, max, 7, err);
    REQUIRE(t != nullptr);
    return t;
}
} // namespace

TEST_CASE("filter accepts data matrix") {
    FilterSpec s;
    s.symbol_ids = {5, 999};
    s.msg_types = {1, 2};
    s.topic_ids = {1, 32767};
    auto t = make(s);
    CHECK(t->generation == 7);
    CHECK(t->accept(hdr(Kind::DATA, 1, 5, 1)));
    CHECK(t->accept(hdr(Kind::DATA, 2, 999, 32767)));
    CHECK_FALSE(t->accept(hdr(Kind::DATA, 3, 5, 1)));     // msg type
    CHECK_FALSE(t->accept(hdr(Kind::DATA, 1, 6, 1)));     // symbol
    CHECK_FALSE(t->accept(hdr(Kind::DATA, 1, 5, 2)));     // topic
    CHECK_FALSE(t->accept(hdr(Kind::DATA, 1, 5, 0)));     // topic 0 (agent-wide) is never data
    CHECK_FALSE(t->accept(hdr(Kind::DATA, 1, 5, 40000))); // topic id out of range
    CHECK_FALSE(t->accept(hdr(Kind::DATA, 100, 5, 1)));   // msg type >= 64
}

TEST_CASE("filter symbol out of range rejects") {
    FilterSpec s;
    s.symbol_ids = {1};
    s.msg_types = {1};
    s.topic_ids = {1};
    auto t = make(s, 100);
    CHECK_FALSE(t->accept(hdr(Kind::DATA, 1, 128, 1)));
    CHECK_FALSE(t->accept(hdr(Kind::DATA, 1, 0xFFFFFFFFu, 1)));
}

TEST_CASE("filter all_symbols and venues") {
    FilterSpec s;
    s.all_symbols = true;
    s.msg_types = {1};
    s.topic_ids = {1};
    auto t = make(s);
    CHECK(t->accept(hdr(Kind::DATA, 1, 123456, 1, 200))); // all venues, id >= 64
    s.venue_ids = {2};
    t = make(s);
    CHECK(t->accept(hdr(Kind::DATA, 1, 1, 1, 2)));
    CHECK_FALSE(t->accept(hdr(Kind::DATA, 1, 1, 1, 1)));
    CHECK_FALSE(t->accept(hdr(Kind::DATA, 1, 1, 1, 200)));
}

TEST_CASE("filter kinds") {
    FilterSpec s;
    s.topic_ids = {2};
    auto t = make(s);
    CHECK(t->accept(hdr(Kind::AGENT_CONTROL, 0xA0, 0, 40))); // whatever the topic
    CHECK(t->accept(hdr(Kind::BUS_ADMIN, 0x80, 0, 2)));
    CHECK_FALSE(t->accept(hdr(Kind::BUS_ADMIN, 0x80, 0, 3)));
    CHECK_FALSE(t->accept(hdr(Kind::PAD, 0xFE, 0, 2)));
    s.bus_admin = false;
    s.agent_control = false;
    t = make(s);
    CHECK_FALSE(t->accept(hdr(Kind::BUS_ADMIN, 0x80, 0, 2)));
    CHECK_FALSE(t->accept(hdr(Kind::AGENT_CONTROL, 0xA0, 0, 2)));
}

TEST_CASE("filter build errors") {
    std::string err;
    FilterSpec s;
    s.symbol_ids = {10};
    CHECK(build_filter(s, 10, 1, err) == nullptr);
    CHECK_FALSE(err.empty());
    s = {}; s.msg_types = {64};
    err.clear();
    CHECK(build_filter(s, 10, 1, err) == nullptr);
    CHECK_FALSE(err.empty());
    s = {}; s.topic_ids = {0};
    err.clear();
    CHECK(build_filter(s, 10, 1, err) == nullptr);
    CHECK_FALSE(err.empty());
    s = {}; s.topic_ids = {32768};
    err.clear();
    CHECK(build_filter(s, 10, 1, err) == nullptr);
    CHECK_FALSE(err.empty());
    s = {}; s.venue_ids = {64};
    err.clear();
    CHECK(build_filter(s, 10, 1, err) == nullptr);
    CHECK_FALSE(err.empty());
}
