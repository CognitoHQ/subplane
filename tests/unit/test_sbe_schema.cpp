#include <doctest.h>

#include <fstream>
#include <sstream>
#include <string>

#include "subplane/decoder/sbe_schema.h"

using namespace subplane;

namespace {
std::string sample_path() {
    std::string f = __FILE__;
    return f.substr(0, f.find_last_of('/')) + "/../data/sample_md.xml";
}
std::string read_sample() {
    std::ifstream in(sample_path());
    std::stringstream ss;
    ss << in.rdbuf();
    return ss.str();
}
} // namespace

TEST_CASE("schema: load sample, header and basics") {
    SbeSchema s;
    std::string err;
    REQUIRE_MESSAGE(s.load_file(sample_path(), err), err);
    CHECK(s.schema_id() == 7);
    CHECK(s.version() == 2);
    CHECK(s.little_endian());
    const auto& h = s.header();
    CHECK(h.size == 8);
    CHECK(h.block_length_off == 0);
    CHECK(h.template_id_off == 2);
    CHECK(h.schema_id_off == 4);
    CHECK(h.version_off == 6);
    CHECK(h.template_id_size == 2);
    CHECK(s.messages().size() == 6);
    CHECK(s.find_message(999) == nullptr);
}

TEST_CASE("schema: root field offsets") {
    SbeSchema s;
    std::string err;
    REQUIRE_MESSAGE(s.load_file(sample_path(), err), err);

    const MessageDef* t = s.find_message(1);
    REQUIRE(t);
    CHECK(t->name == "Trade");
    CHECK(t->block_length == 39);
    struct E { const char* n; uint16_t off, size; SbePrim p; };
    const E exp[] = {{"SecurityID", 0, 8, SbePrim::U64}, {"SeqNum", 8, 8, SbePrim::U64},
                     {"TransactTime", 16, 8, SbePrim::U64}, {"MarketSegmentID", 24, 2, SbePrim::U16},
                     {"Price", 26, 8, SbePrim::I64}, {"Qty", 34, 4, SbePrim::U32}, {"Side", 38, 1, SbePrim::U8}};
    for (const auto& e : exp) {
        const FieldDef* f = s.find_field(*t, e.n);
        REQUIRE_MESSAGE(f, e.n);
        CHECK(f->offset == e.off);
        CHECK(f->size == e.size);
        CHECK(f->prim == e.p);
    }

    const MessageDef* q = s.find_message(2);
    REQUIRE(q);
    CHECK(s.find_field(*q, "BidPx")->offset == 32); // explicit offset
    CHECK(s.find_field(*q, "BidQty")->offset == 40);
    CHECK(s.find_field(*q, "AskPx")->offset == 44);
    CHECK(q->block_length == 56);

    const MessageDef* k = s.find_message(4);
    REQUIRE(k);
    const FieldDef* sym = s.find_field(*k, "Symbol");
    REQUIRE(sym);
    CHECK(sym->prim == SbePrim::CHAR_ARRAY);
    CHECK(sym->length == 8);
    CHECK(sym->size == 8);
    CHECK(s.find_field(*k, "SeqNum")->offset == 8);
}

TEST_CASE("schema: groups, data and nested-field detection") {
    SbeSchema s;
    std::string err;
    REQUIRE_MESSAGE(s.load_file(sample_path(), err), err);
    const MessageDef* b = s.find_message(3);
    REQUIRE(b);
    CHECK(b->block_length == 26);
    REQUIRE(b->groups.size() == 1);
    const GroupDef& g = b->groups[0];
    CHECK(g.name == "Levels");
    CHECK(g.dim.size == 4);
    CHECK(g.block_length == 13);
    REQUIRE(g.groups.size() == 1);
    CHECK(g.groups[0].block_length == 12);
    REQUIRE(b->data.size() == 1);
    CHECK(b->data[0].hdr_size == 2);
    CHECK(b->data[0].len_size == 2);

    CHECK(s.find_field(*b, "Px") == nullptr);
    CHECK(s.is_nested_field(*b, "Px"));
    CHECK(s.is_nested_field(*b, "OrderId"));
    CHECK(s.is_nested_field(*b, "Note"));
    CHECK_FALSE(s.is_nested_field(*b, "SeqNum"));
    CHECK_FALSE(s.is_nested_field(*b, "Nope"));
}

TEST_CASE("schema: big endian attribute") {
    std::string x = read_sample();
    auto p = x.find("littleEndian");
    REQUIRE(p != std::string::npos);
    x.replace(p, 12, "bigEndian");
    SbeSchema s;
    std::string err;
    REQUIRE_MESSAGE(s.load_string(x, err), err);
    CHECK_FALSE(s.little_endian());
}

TEST_CASE("schema: errors") {
    SbeSchema s;
    std::string err;
    CHECK_FALSE(s.load_string("<not xml", err));
    CHECK(err.find("XML") != std::string::npos);

    CHECK_FALSE(s.load_file("/nonexistent/schema.xml", err));

    std::string x = read_sample();
    auto p = x.find("name=\"Qty32\" primitiveType=\"uint32\"");
    REQUIRE(p != std::string::npos);
    x.replace(p, 35, "name=\"Qty32\" primitiveType=\"uint48\"");
    CHECK_FALSE(s.load_string(x, err));
    CHECK(err.find("uint48") != std::string::npos);

    CHECK_FALSE(s.load_string("<sbe:messageSchema xmlns:sbe=\"x\" id=\"1\" version=\"0\"><types/></sbe:messageSchema>", err));
}
