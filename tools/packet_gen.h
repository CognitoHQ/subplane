#pragma once

// Synthetic SBE packet generator shared by sp_sbe_sender and sp_replay --bench.
// Builds datagrams for any schema laid out by a `framing:` block. Cold-path
// tool code (the generator itself is not on the agent's hot path).

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "subplane/core/clock.h"
#include "sbe_encoder.h"
#include "tool_common.h"

#ifndef SP_DEFAULT_SCHEMA
#define SP_DEFAULT_SCHEMA "tests/data/sample_md.xml"
#endif

namespace sptool {

using namespace subplane;

struct GenOptions {
    std::string schema = SP_DEFAULT_SCHEMA, framing, group, host = "127.0.0.1", interface;
    uint64_t port = 0, rate = 0, count = 0, symbols = 10, per_packet = 1, heartbeat_ms = 0;
    uint64_t gap_every = 0, seq_reset_at = 0, unknown_every = 0, dup_every = 0;
    unsigned mix_trade = 70, mix_quote = 25, mix_book = 5;
};

bool parse_mix(const std::string& s, GenOptions& o, std::string& err) {
    o.mix_trade = o.mix_quote = o.mix_book = 0;
    size_t i = 0;
    while (i < s.size()) {
        size_t comma = s.find(',', i);
        if (comma == std::string::npos) comma = s.size();
        const std::string item = s.substr(i, comma - i);
        const size_t eq = item.find('=');
        if (eq == std::string::npos) { err = "bad --mix item: " + item; return false; }
        const std::string name = item.substr(0, eq);
        const unsigned long v = std::strtoul(item.c_str() + eq + 1, nullptr, 10);
        if (name == "trade") o.mix_trade = static_cast<unsigned>(v);
        else if (name == "quote") o.mix_quote = static_cast<unsigned>(v);
        else if (name == "book") o.mix_book = static_cast<unsigned>(v);
        else { err = "unknown --mix template: " + name; return false; }
        i = comma + 1;
    }
    if (o.mix_trade + o.mix_quote + o.mix_book == 0) { err = "--mix sums to zero"; return false; }
    return true;
}

const FieldDef* find_f(const std::vector<FieldDef>& v, std::string_view name) {
    for (const auto& f : v) if (f.name == name) return &f;
    return nullptr;
}
// First of several names that exists: the sample schema (SecurityID, SeqNum, ...) and the
// canonical schema (InstrumentId, Sequence, ...) are both generated from the same code.
const FieldDef* find_f(const std::vector<FieldDef>& v, std::string_view a, std::string_view b) {
    const FieldDef* f = find_f(v, a);
    return f ? f : find_f(v, b);
}
const GroupDef* find_g(const std::vector<GroupDef>& v, std::string_view name) {
    for (const auto& g : v) if (g.name == name) return &g;
    return nullptr;
}

// Field handles resolved once at startup so the send loop does no name lookups.
struct Tpl {
    const MessageDef* def = nullptr;
    const FieldDef *sec = nullptr, *seq = nullptr, *ts = nullptr, *seg = nullptr, *px = nullptr, *qty = nullptr,
                   *side = nullptr, *bidpx = nullptr, *bidqty = nullptr, *askpx = nullptr, *askqty = nullptr,
                   *newseq = nullptr, *seqfield = nullptr;
    const GroupDef *levels = nullptr, *orders = nullptr;
    const FieldDef *l_px = nullptr, *l_qty = nullptr, *l_side = nullptr, *o_id = nullptr, *o_qty = nullptr;
    const FieldDef *action = nullptr, *level = nullptr; // canonical BookUpdate (flat, one level)
    const VarDataDef* note = nullptr;
};

enum class GenKind { TRADE, QUOTE, BOOK, HEARTBEAT, SEQRESET, UNKNOWN };

struct GenStats {
    uint64_t packets = 0, messages = 0, bytes = 0, gaps = 0, dups = 0, send_errors = 0;
};

class Generator {
public:
    Generator(const SbeSchema& s, const FramingConfig& c, const GenOptions& o)
        : s_(s), cfg_(c), o_(o), little_(s.little_endian()), b_(s), pkt_(9000) {}

    bool init(std::string& err) {
        auto bind_tpl = [&](Tpl& t, uint16_t id, bool needed, const char* what) {
            t.def = s_.find_message(id);
            if (!t.def && needed) { err = std::string("schema has no ") + what + " template " + std::to_string(id); return false; }
            if (!t.def) return true;
            const auto& f = t.def->fields;
            t.sec = find_f(f, "SecurityID", "InstrumentId"); t.seq = find_f(f, "SeqNum", "Sequence"); t.ts = find_f(f, "TransactTime", "ExchangeTs");
            t.seg = find_f(f, "MarketSegmentID", "VenueId"); t.px = find_f(f, "Price"); t.qty = find_f(f, "Qty");
            t.side = find_f(f, "Side", "Aggressor"); t.bidpx = find_f(f, "BidPx"); t.bidqty = find_f(f, "BidQty");
            t.askpx = find_f(f, "AskPx"); t.askqty = find_f(f, "AskQty"); t.newseq = find_f(f, "NewSeq");
            t.action = find_f(f, "Action"); t.level = find_f(f, "Level");
            if (cfg_.seq_source == SeqSource::MESSAGE_FIELD) {
                t.seqfield = find_f(f, cfg_.seq_field);
                if (!t.seqfield) { err = std::string(what) + " has no sequence field " + cfg_.seq_field; return false; }
            }
            t.levels = find_g(t.def->groups, "Levels");
            if (t.levels) {
                t.orders = find_g(t.levels->groups, "Orders");
                t.l_px = find_f(t.levels->fields, "Px"); t.l_qty = find_f(t.levels->fields, "LevelQty");
                t.l_side = find_f(t.levels->fields, "LevelSide");
                if (t.orders) { t.o_id = find_f(t.orders->fields, "OrderId"); t.o_qty = find_f(t.orders->fields, "OrderQty"); }
            }
            for (const auto& d : t.def->data) if (d.name == "Note") t.note = &d;
            return true;
        };
        if (!bind_tpl(trade_, 1, o_.mix_trade > 0, "Trade") || !bind_tpl(quote_, 2, o_.mix_quote > 0, "Quote") ||
            !bind_tpl(book_, 3, o_.mix_book > 0, "BookUpdate") ||
            !bind_tpl(hb_, 100, o_.heartbeat_ms > 0, "Heartbeat") ||
            !bind_tpl(reset_, 101, o_.seq_reset_at > 0, "SeqReset")) return false;
        // Canonical prices are int64 scaled by 1e-9: make the synthetic numbers read as sane prices/sizes.
        canonical_ = s_.package() == "subplane.canonical";

        auto find_ff = [](const std::vector<FramingField>& v, const std::string& n) -> const FramingField* {
            for (const auto& f : v) if (f.name == n) return &f;
            return nullptr;
        };
        if (cfg_.seq_source == SeqSource::PACKET_HEADER) seqf_ = find_ff(cfg_.packet_header, cfg_.seq_field);
        if (cfg_.has_count) countf_ = find_ff(cfg_.packet_header, cfg_.count_field);
        if (cfg_.len_source == LenSource::MESSAGE_PREFIX) lenf_ = find_ff(cfg_.message_prefix, cfg_.len_field);
        return true;
    }

    // Builds packet number k (1-based) into the internal buffer; returns its size, 0 on overflow.
    uint32_t build(uint64_t k, bool heartbeat_due) {
        uint8_t* p = pkt_.data();
        const auto cap = static_cast<uint32_t>(pkt_.size());
        uint32_t pos = cfg_.packet_header_size;
        if (pos > cap) return 0;
        std::memset(p, 0, pos);
        for (const auto& f : cfg_.packet_header) if (f.type == FieldType::BYTES) std::memset(p + f.offset, 'S', f.size);

        if (o_.gap_every && k % o_.gap_every == 0) {
            // INTENT: skip exactly one sequence number so receivers see a gap of 1.
            if (cfg_.seq_source == SeqSource::PACKET_HEADER) ++next_pkt_seq_;
            else if (cfg_.seq_source == SeqSource::MESSAGE_FIELD) ++msg_seq_;
            ++st_.gaps;
        }
        const uint64_t pkt_seq = next_pkt_seq_;
        nmsg_ = 0;

        for (uint64_t i = 0; i < o_.per_packet; ++i) {
            const uint64_t r = rnd() % (static_cast<uint64_t>(o_.mix_trade) + o_.mix_quote + o_.mix_book);
            GenKind kind = r < o_.mix_trade ? GenKind::TRADE : (r < o_.mix_trade + o_.mix_quote ? GenKind::QUOTE : GenKind::BOOK);
            if (!append(kind, p, pos, cap)) return 0;
        }
        if (heartbeat_due && !append(GenKind::HEARTBEAT, p, pos, cap)) return 0;
        const bool do_reset = o_.seq_reset_at && k == o_.seq_reset_at;
        if (do_reset && !append(GenKind::SEQRESET, p, pos, cap)) return 0;
        if (o_.unknown_every && k % o_.unknown_every == 0 && !append(GenKind::UNKNOWN, p, pos, cap)) return 0;

        if (seqf_) write_framing(p, *seqf_, pkt_seq);
        if (countf_) write_framing(p, *countf_, nmsg_);
        next_pkt_seq_ = pkt_seq + 1;
        if (do_reset) { next_pkt_seq_ = 1; msg_seq_ = 0; }
        ++st_.packets;
        st_.messages += nmsg_;
        st_.bytes += pos;
        return pos;
    }

    const uint8_t* data() const { return pkt_.data(); }
    GenStats& stats() { return st_; }

private:
    uint64_t rnd() { rng_ ^= rng_ << 13; rng_ ^= rng_ >> 7; rng_ ^= rng_ << 17; return rng_; }

    void write_framing(uint8_t* base, const FramingField& f, uint64_t v) const {
        const bool big = f.type == FieldType::U16BE || f.type == FieldType::U32BE || f.type == FieldType::U64BE;
        put_uint(base + f.offset, f.size, v, !big);
    }
    void setf(uint8_t* block, const FieldDef* f, uint64_t v) const { if (f) write_field(block, *f, v, little_); }

    bool append(GenKind kind, uint8_t* p, uint32_t& pos, uint32_t cap) {
        const uint32_t pre = cfg_.message_prefix_size;
        if (pos + pre >= cap) return false;
        std::memset(p + pos, 0, pre);
        uint8_t* m = p + pos + pre;
        const uint32_t mcap = cap - pos - pre;

        if (kind == GenKind::UNKNOWN) {
            b_.begin_raw(999, 4, m, mcap);
        } else {
            const Tpl& t = kind == GenKind::TRADE ? trade_ : kind == GenKind::QUOTE ? quote_ : kind == GenKind::BOOK ? book_
                         : kind == GenKind::HEARTBEAT ? hb_ : reset_;
            b_.begin(*t.def, m, mcap);
            if (!b_.ok()) return false;
            uint8_t* root = b_.root();
            const uint64_t sv = cfg_.seq_source == SeqSource::MESSAGE_FIELD ? ++msg_seq_ : ++msg_no_;
            const uint64_t sym = 1 + (sym_n_++ % o_.symbols);
            const uint64_t px = canonical_ ? (100000ull + sym * 1000ull + rnd() % 500) * 1000000ull /* 100.xxx */ : 1000000 + sym * 100 + rnd() % 50;
            const uint64_t qscale = canonical_ ? 1000000000ull : 1;
            setf(root, t.seq, sv);
            setf(root, t.seqfield, sv);
            setf(root, t.sec, sym);
            setf(root, t.ts, now_ns());
            setf(root, t.seg, 1);
            switch (kind) {
                case GenKind::TRADE:
                    setf(root, t.px, px); setf(root, t.qty, (1 + rnd() % 500) * qscale); setf(root, t.side, 1 + rnd() % 2);
                    break;
                case GenKind::QUOTE:
                    setf(root, t.bidpx, px); setf(root, t.bidqty, (1 + rnd() % 500) * qscale);
                    setf(root, t.askpx, px + (canonical_ ? 1000000 : 1)); setf(root, t.askqty, (1 + rnd() % 500) * qscale);
                    break;
                case GenKind::BOOK: {
                    if (!t.levels) {
                        // Flat one-level update (canonical BookUpdate).
                        setf(root, t.px, px); setf(root, t.qty, (1 + rnd() % 500) * qscale); setf(root, t.side, 1 + rnd() % 2);
                        setf(root, t.action, 1 + rnd() % 3); setf(root, t.level, rnd() % 10);
                    }
                    if (t.levels) {
                        const auto levels = static_cast<uint16_t>(1 + rnd() % 3);
                        b_.begin_group(*t.levels, levels);
                        for (uint16_t l = 0; l < levels; ++l) {
                            uint8_t* e = b_.entry(*t.levels);
                            if (!e) break;
                            setf(e, t.l_px, px - l); setf(e, t.l_qty, 1 + rnd() % 500); setf(e, t.l_side, 1 + (l & 1U));
                            if (t.orders) {
                                const auto orders = static_cast<uint16_t>(rnd() % 3);
                                b_.begin_group(*t.orders, orders);
                                for (uint16_t q = 0; q < orders; ++q) {
                                    uint8_t* oe = b_.entry(*t.orders);
                                    if (!oe) break;
                                    setf(oe, t.o_id, rnd() & 0xFFFFFFU); setf(oe, t.o_qty, 1 + rnd() % 100);
                                }
                            }
                        }
                    }
                    if (t.note) b_.var_data(*t.note, "synthetic");
                    break;
                }
                case GenKind::HEARTBEAT:
                    break;
                case GenKind::SEQRESET:
                    setf(root, t.newseq, 1);
                    break;
                case GenKind::UNKNOWN:
                    break;
            }
        }
        if (!b_.ok()) return false;
        const uint32_t mlen = b_.size();
        if (lenf_) write_framing(p + pos, *lenf_, mlen + (cfg_.length_includes_prefix ? pre : 0U));
        pos += pre + mlen;
        ++nmsg_;
        return true;
    }

    const SbeSchema& s_;
    const FramingConfig& cfg_;
    const GenOptions& o_;
    bool little_;
    MessageBuilder b_;
    std::vector<uint8_t> pkt_;
    Tpl trade_, quote_, book_, hb_, reset_;
    const FramingField *seqf_ = nullptr, *countf_ = nullptr, *lenf_ = nullptr;
    uint64_t next_pkt_seq_ = 1, msg_seq_ = 0, msg_no_ = 0, sym_n_ = 0, nmsg_ = 0;
    bool canonical_ = false;
    uint64_t rng_ = 0x9E3779B97F4A7C15ULL;
    GenStats st_;
};

} // namespace sptool
