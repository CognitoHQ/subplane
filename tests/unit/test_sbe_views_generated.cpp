// Uses the header sp_schema generated at build time for tests/data/sample_md.xml:
// its static_asserts are the compile-time proof; here the views read messages
// built by the same encoder sp_sbe_sender uses.
#include <doctest.h>

#include "sample_md_views.h"
#include "sbe_encoder.h"

#include <cstring>
#include <string>

using namespace subplane;

namespace {
std::string sample_path() { std::string f = __FILE__; return f.substr(0, f.find_last_of('/')) + "/../data/sample_md.xml"; }
}

TEST_CASE("generated views: struct form (Quote), view form (Trade), header checks") {
    static_assert(sp_views::Ticker::kIsStruct);
    static_assert(sp_views::Heartbeat::kIsStruct);
    static_assert(!sp_views::Trade::kIsStruct);
    static_assert(!sp_views::Quote::kIsStruct);
    static_assert(sp_views::kSchemaId == 7);
    static_assert(sp_views::kHeaderSize == 8);
    static_assert(sizeof(sp_views::Ticker) == 32);
    static_assert(sp_views::Trade::kSize == 47);
    static_assert(sp_views::Quote::kSize == 64);

    SbeSchema s; std::string err;
    REQUIRE_MESSAGE(s.load_file(sample_path(), err), err);
    uint8_t buf[256];
    sptool::MessageBuilder b(s);

    const MessageDef& trade = *s.find_message(1);
    b.begin(trade, buf, sizeof buf);
    REQUIRE(sptool::set_u64(s, buf, trade, "SecurityID", 424242));
    REQUIRE(sptool::set_u64(s, buf, trade, "SeqNum", 77));
    REQUIRE(sptool::set_u64(s, buf, trade, "TransactTime", 1758742200000000000ull));
    REQUIRE(sptool::set_u64(s, buf, trade, "MarketSegmentID", 3));
    REQUIRE(sptool::set_u64(s, buf, trade, "Price", static_cast<uint64_t>(-12345)));
    REQUIRE(sptool::set_u64(s, buf, trade, "Qty", 500));
    REQUIRE(sptool::set_u64(s, buf, trade, "Side", 2));
    REQUIRE(b.ok());
    REQUIRE(b.size() == 47);
    auto t = sp_views::view<sp_views::Trade>(buf, b.size());
    REQUIRE(t.has_value());
    CHECK(t->templateId() == 1);
    CHECK(t->schemaId() == 7);
    CHECK(t->blockLength() == 39);
    CHECK(t->SecurityID() == 424242);
    CHECK(t->SeqNum() == 77);
    CHECK(t->TransactTime() == 1758742200000000000ull);
    CHECK(t->MarketSegmentID() == 3);
    CHECK(t->Price() == -12345);
    CHECK(t->Qty() == 500);
    CHECK(t->Side() == 2);
    CHECK_FALSE(sp_views::view<sp_views::Trade>(buf, 46).has_value());   // short
    CHECK_FALSE(sp_views::view<sp_views::Quote>(buf, 64).has_value());   // wrong template
    CHECK(sp_views::view<sp_views::Heartbeat>(buf, b.size()) == nullptr);   // wrong template, struct form

    const MessageDef& quote = *s.find_message(2);
    b.begin(quote, buf, sizeof buf);
    REQUIRE(sptool::set_u64(s, buf, quote, "SecurityID", 9));
    REQUIRE(sptool::set_u64(s, buf, quote, "MarketSegmentID", 65535));
    REQUIRE(sptool::set_u64(s, buf, quote, "BidPx", 100));
    REQUIRE(sptool::set_u64(s, buf, quote, "BidQty", 11));
    REQUIRE(sptool::set_u64(s, buf, quote, "AskPx", 101));
    REQUIRE(sptool::set_u64(s, buf, quote, "AskQty", 12));
    REQUIRE(b.size() == 64);
    auto q = sp_views::view<sp_views::Quote>(buf, b.size());
    REQUIRE(q.has_value());
    CHECK(q->data() == buf);
    CHECK(q->templateId() == 2);
    CHECK(q->SecurityID() == 9);
    CHECK(q->MarketSegmentID() == 65535);
    CHECK(q->BidPx() == 100);     // after the 6-byte gap
    CHECK(q->BidQty() == 11);
    CHECK(q->AskPx() == 101);
    CHECK(q->AskQty() == 12);
    buf[4] = 8; // schema id 8: refused
    CHECK_FALSE(sp_views::view<sp_views::Quote>(buf, b.size()).has_value());

    const MessageDef& ticker = *s.find_message(4);
    b.begin(ticker, buf, sizeof buf);
    REQUIRE(sptool::write_chars(buf + s.header().size, *s.find_field(ticker, "Symbol"), "ESZ6"));
    REQUIRE(sptool::set_u64(s, buf, ticker, "SeqNum", 5));
    REQUIRE(sptool::set_u64(s, buf, ticker, "TransactTime", 6));
    REQUIRE(b.size() == 32);
    const sp_views::Ticker* k = sp_views::view<sp_views::Ticker>(buf, b.size());
    REQUIRE(k != nullptr);
    CHECK(reinterpret_cast<const uint8_t*>(k) == buf);
    CHECK(k->templateId == 4);
    CHECK(std::string(k->Symbol, 4) == "ESZ6");
    CHECK(k->SeqNum == 5);
    CHECK(k->TransactTime == 6);

    const MessageDef& hb = *s.find_message(100);
    b.begin(hb, buf, sizeof buf);
    REQUIRE(sptool::set_u64(s, buf, hb, "SeqNum", 99));
    const sp_views::Heartbeat* h = sp_views::view<sp_views::Heartbeat>(buf, b.size());
    REQUIRE(h != nullptr);
    CHECK(h->SeqNum == 99);
}
