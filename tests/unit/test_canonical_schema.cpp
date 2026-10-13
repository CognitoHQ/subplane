// The canonical flat schema: every message flat and naturally aligned (struct
// form), fixed offsets for the common prefix, and the generated sp_canonical
// header reading messages built by the test encoder.
#include <doctest.h>

#include "subplane/decoder/sbe_views.h"
#include "subplane_canonical_views.h"
#include "sbe_encoder.h"

#include <cstring>
#include <string>

using namespace subplane;

namespace {
std::string schema_path() { std::string f = __FILE__; return f.substr(0, f.find_last_of('/')) + "/../../schemas/subplane_canonical.xml"; }
const ViewSlot* slot(const FlatLayout& l, const char* name) { for (const auto& v : l.slots) if (v.name == name) return &v; return nullptr; }
}

TEST_CASE("canonical: every message is flat, aligned, and carries the same 32-byte prefix") {
    SbeSchema s; std::string err;
    REQUIRE_MESSAGE(s.load_file(schema_path(), err), err);
    CHECK(s.schema_id() == 100);
    CHECK(s.version() == 1);
    CHECK(s.package() == "subplane.canonical");
    CHECK(s.little_endian());
    struct Exp { uint16_t id; const char* name; uint32_t size; };
    const Exp exp[] = {{1, "Trade", 72}, {2, "Quote", 72}, {3, "BookUpdate", 64}, {4, "BookSnapshot", 368}, {5, "Ohlcv", 88},
                       {6, "Status", 48}, {10, "InstrumentDefinition", 144}, {100, "Heartbeat", 40}};
    REQUIRE(s.messages().size() == 8);
    for (const Exp& e : exp) {
        const MessageDef* m = s.find_message(e.id);
        REQUIRE_MESSAGE(m != nullptr, e.name);
        CHECK(m->name == e.name);
        FlatLayout l; std::string reason;
        REQUIRE_MESSAGE(flat_layout(s, *m, l, reason), reason);
        const std::string why = std::string(e.name) + ": " + l.misaligned_field;
        CHECK_MESSAGE(l.aligned, why);
        CHECK(l.size == e.size);
        CHECK(slot(l, "InstrumentId")->offset == 8);
        CHECK(slot(l, "VenueId")->offset == 12);
        CHECK(slot(l, "Flags")->offset == 14);
        CHECK(slot(l, "Sequence")->offset == 16);
        CHECK(slot(l, "ExchangeTs")->offset == 24);
        CHECK(slot(l, "RecvTs")->offset == 32);
        if (e.id != 6 && e.id != 100) { REQUIRE(l.constants.size() == 1); CHECK(l.constants[0] == "PriceExponent (PriceExponent)"); }
    }
    FlatLayout snap; std::string reason;
    REQUIRE(flat_layout(s, *s.find_message(4), snap, reason));
    const ViewSlot* bid = slot(snap, "BidPx");
    REQUIRE(bid); CHECK(bid->offset == 40); CHECK(bid->size == 80); CHECK(bid->length == 10); CHECK(bid->elem == SbePrim::I64);
    CHECK(slot(snap, "AskQty")->offset == 280);
    CHECK(slot(snap, "Depth")->offset == 360);

    ViewsOptions o; o.ns = "sp_canonical"; ViewsReport rep; std::string out;
    REQUIRE_MESSAGE(emit_cpp(s, o, out, rep, err), err);
    CHECK(rep.emitted.size() == 8);
    CHECK(rep.skipped.empty());
    CHECK(out.find("\nclass ") == std::string::npos);      // every message is a struct (the header comment mentions "view class")
    CHECK(out.find("int64_t BidPx[10];") != std::string::npos);
    REQUIRE_MESSAGE(emit_numpy(s, o, out, rep, err), err);
    CHECK(out.find("'('<i8', (10,))'") == std::string::npos); // dtype formats are not quoted tuples
    CHECK(out.find("('<i8', (10,))") != std::string::npos);
    CHECK(out.find("FORMAT = '<HHHHIHHQQQ10q10q10q10qB7x'") != std::string::npos);
    REQUIRE_MESSAGE(emit_q(s, o, out, rep, err), err);
    CHECK(out.find("d[`BidPx]:first (enlist \"j\";enlist 8) 1: b[40+til 80];") != std::string::npos);
}

TEST_CASE("canonical: generated struct views read encoded messages") {
    using namespace sp_canonical;
    static_assert(Trade::kIsStruct && Quote::kIsStruct && BookUpdate::kIsStruct && BookSnapshot::kIsStruct &&
                  Ohlcv::kIsStruct && Status::kIsStruct && InstrumentDefinition::kIsStruct && Heartbeat::kIsStruct);
    static_assert(sizeof(Trade) == 72 && sizeof(BookSnapshot) == 368 && sizeof(InstrumentDefinition) == 144 && sizeof(Heartbeat) == 40);
    static_assert(offsetof(Trade, Sequence) == 16 && offsetof(Quote, Sequence) == 16 && offsetof(BookSnapshot, Sequence) == 16);
    static_assert(offsetof(BookSnapshot, AskQty) == 280);
    static_assert(kSchemaId == 100);

    SbeSchema s; std::string err;
    REQUIRE(s.load_file(schema_path(), err));
    uint8_t buf[512];
    sptool::MessageBuilder b(s);

    const MessageDef& trade = *s.find_message(1);
    b.begin(trade, buf, sizeof buf);
    REQUIRE(sptool::set_u64(s, buf, trade, "InstrumentId", 7));
    REQUIRE(sptool::set_u64(s, buf, trade, "VenueId", 3));
    REQUIRE(sptool::set_u64(s, buf, trade, "Sequence", 1001));
    REQUIRE(sptool::set_u64(s, buf, trade, "ExchangeTs", 1758742200000000000ull));
    REQUIRE(sptool::set_u64(s, buf, trade, "Price", 65432100000000ull));   // 65432.1
    REQUIRE(sptool::set_u64(s, buf, trade, "Qty", 250000000ull));           // 0.25
    REQUIRE(sptool::set_u64(s, buf, trade, "TradeId", 99));
    REQUIRE(sptool::set_u64(s, buf, trade, "Aggressor", 2));
    REQUIRE(b.size() == 72);
    const Trade* t = view<Trade>(buf, b.size());
    REQUIRE(t != nullptr);
    CHECK(reinterpret_cast<const uint8_t*>(t) == buf);
    CHECK(t->templateId == 1); CHECK(t->schemaId == 100); CHECK(t->blockLength == 64);
    CHECK(t->InstrumentId == 7); CHECK(t->VenueId == 3); CHECK(t->Sequence == 1001);
    CHECK(t->ExchangeTs == 1758742200000000000ull);
    CHECK(t->Price == 65432100000000); CHECK(t->Qty == 250000000); CHECK(t->TradeId == 99); CHECK(t->Aggressor == 2);
    CHECK(view<Quote>(buf, b.size()) == nullptr);
    CHECK(view<Trade>(buf, 71) == nullptr);

    const MessageDef& snap = *s.find_message(4);
    b.begin(snap, buf, sizeof buf);
    REQUIRE(sptool::set_u64(s, buf, snap, "InstrumentId", 7));
    REQUIRE(sptool::set_u64(s, buf, snap, "Depth", 10));
    const FieldDef* bidpx = s.find_field(snap, "BidPx");
    REQUIRE(bidpx); CHECK(bidpx->prim == SbePrim::OPAQUE); CHECK(bidpx->elem_prim == SbePrim::I64); CHECK(bidpx->length == 10);
    for (int i = 0; i < 10; ++i) { const int64_t px = 100000000000ll - i * 1000000000ll; std::memcpy(buf + 8 + bidpx->offset + 8 * i, &px, 8); }
    REQUIRE(b.size() == 368);
    const BookSnapshot* bs = view<BookSnapshot>(buf, b.size());
    REQUIRE(bs != nullptr);
    CHECK(bs->Depth == 10);
    CHECK(bs->BidPx[0] == 100000000000ll);
    CHECK(bs->BidPx[9] == 91000000000ll);
    CHECK(bs->AskPx[0] == 0);

    const MessageDef& def = *s.find_message(10);
    b.begin(def, buf, sizeof buf);
    REQUIRE(sptool::set_u64(s, buf, def, "InstrumentId", 7));
    REQUIRE(sptool::write_chars(buf + 8, *s.find_field(def, "Symbol"), "BTC-USD"));
    REQUIRE(sptool::write_chars(buf + 8, *s.find_field(def, "Exchange"), "COINBASE"));
    REQUIRE(sptool::set_u64(s, buf, def, "TickSize", 10000000ull));
    REQUIRE(sptool::set_u64(s, buf, def, "Status", 1));
    REQUIRE(b.size() == 144);
    const InstrumentDefinition* d = view<InstrumentDefinition>(buf, b.size());
    REQUIRE(d != nullptr);
    CHECK(std::string(d->Symbol, 7) == "BTC-USD");
    CHECK(d->Symbol[7] == 0);
    CHECK(std::string(d->Exchange, 8) == "COINBASE");
    CHECK(d->TickSize == 10000000);
    CHECK(d->Status == 1);

    const MessageDef& hb = *s.find_message(100);
    b.begin(hb, buf, sizeof buf);
    REQUIRE(sptool::set_u64(s, buf, hb, "Sequence", 5));
    const Heartbeat* h = view<Heartbeat>(buf, b.size());
    REQUIRE(h != nullptr);
    CHECK(h->Sequence == 5);
    CHECK(h->InstrumentId == 0);
}
