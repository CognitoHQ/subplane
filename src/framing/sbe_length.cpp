#include "flashtick/framing/sbe_length.h"

#include <algorithm>

namespace flashtick {
namespace {

bool emit_group(const GroupDef& g, std::vector<WalkOp>& ops, unsigned depth, std::string& err) {
    if (depth > SBE_MAX_GROUP_DEPTH) {
        err = "group '" + g.name + "' nests deeper than " + std::to_string(SBE_MAX_GROUP_DEPTH);
        return false;
    }
    const size_t begin = ops.size();
    WalkOp b;
    b.kind = WalkOp::GROUP_BEGIN;
    b.dim_block_len_size = g.dim.block_length_size;
    b.dim_num_size = g.dim.num_in_group_size;
    b.dim_block_len_offset = g.dim.block_length_off;
    b.dim_num_offset = g.dim.num_in_group_off;
    b.hdr_size = g.dim.size;
    b.since_version = g.since_version;
    ops.push_back(b);
    for (const auto& s : g.groups)
        if (!emit_group(s, ops, depth + 1, err)) return false;
    for (const auto& d : g.data) {
        WalkOp o;
        o.kind = WalkOp::VAR_DATA;
        o.len_prefix_size = d.len_size;
        o.dim_num_offset = d.len_off;
        o.hdr_size = d.hdr_size;
        o.since_version = d.since_version;
        ops.push_back(o);
    }
    WalkOp e;
    e.kind = WalkOp::GROUP_END;
    e.jump = static_cast<uint16_t>(begin);
    const size_t end = ops.size();
    ops.push_back(e);
    ops[begin].jump = static_cast<uint16_t>(end);
    return true;
}

} // namespace

bool SbeLengthWalker::build(const SbeSchema& schema, std::string& err) {
    header_ = schema.header();
    little_ = schema.little_endian();
    ops_.clear();
    programs_.clear();

    uint16_t max_id = 0;
    for (const auto& m : schema.messages()) max_id = std::max(max_id, m.template_id);
    programs_.resize(static_cast<size_t>(max_id) + 1);

    std::vector<std::pair<uint16_t, std::pair<size_t, size_t>>> spans; // template -> [start,count)
    for (const auto& m : schema.messages()) {
        const size_t start = ops_.size();
        for (const auto& g : m.groups)
            if (!emit_group(g, ops_, 1, err)) { err = "message '" + m.name + "': " + err; return false; }
        for (const auto& d : m.data) {
            WalkOp o;
            o.kind = WalkOp::VAR_DATA;
            o.len_prefix_size = d.len_size;
            o.dim_num_offset = d.len_off;
            o.hdr_size = d.hdr_size;
            o.since_version = d.since_version;
            ops_.push_back(o);
        }
        if (ops_.size() - start > 65535) { err = "walk program too large for '" + m.name + "'"; return false; }
        // INTENT: emit_group records jumps as indices into ops_, but walk_program
        // indexes prog.ops, which starts at `start`. Rebase to program-relative
        // indices. Without this every message after the first grouped one walks
        // at most two group entries and reports a short length (found on real
        // CME MDP 3.0 packets; the unit schema hid it because its grouped
        // template happened to be first).
        for (size_t i = start; i < ops_.size(); ++i)
            if (ops_[i].kind == WalkOp::GROUP_BEGIN || ops_[i].kind == WalkOp::GROUP_END)
                ops_[i].jump = static_cast<uint16_t>(ops_[i].jump - start);
        spans.push_back({m.template_id, {start, ops_.size() - start}});
    }
    // Pointers are taken only after ops_ stops growing.
    static const WalkOp kEmpty{};
    for (const auto& [id, sc] : spans) {
        Entry& en = programs_[id];
        en.valid = true;
        en.prog.ops = sc.second ? ops_.data() + sc.first : &kEmpty;
        en.prog.count = static_cast<uint16_t>(sc.second);
    }
    return true;
}

WalkStatus SbeLengthWalker::walk(const uint8_t* msg, uint32_t avail, uint32_t& total_len) const noexcept {
    total_len = 0;
    if (avail < header_.size) return WalkStatus::TRUNCATED;
    const uint64_t blk = detail::sbe_read_uint(msg + header_.block_length_off, header_.block_length_size, little_);
    const uint64_t tid = detail::sbe_read_uint(msg + header_.template_id_off, header_.template_id_size, little_);
    if (tid > 0xFFFF) return WalkStatus::UNKNOWN_TEMPLATE;
    const WalkProgram* prog = program(static_cast<uint16_t>(tid));
    if (!prog) return WalkStatus::UNKNOWN_TEMPLATE;
    const uint64_t root_end = static_cast<uint64_t>(header_.size) + blk;
    if (root_end > avail) return WalkStatus::TRUNCATED;
    const uint64_t ver = detail::sbe_read_uint(msg + header_.version_off, header_.version_size, little_);
    return walk_program(*prog, msg, avail, static_cast<uint32_t>(root_end),
                        static_cast<uint16_t>(ver > 0xFFFF ? 0xFFFF : ver), total_len);
}

WalkStatus SbeLengthWalker::walk_program(const WalkProgram& prog, const uint8_t* msg, uint32_t avail,
                                         uint32_t pos, uint16_t acting_version, uint32_t& total_len) const noexcept {
    struct Frame { uint64_t remaining; uint64_t block_len; };
    Frame stack[SBE_MAX_GROUP_DEPTH];
    unsigned depth = 0;
    uint32_t i = 0;
    while (i < prog.count) {
        const WalkOp& op = prog.ops[i];
        switch (op.kind) {
            case WalkOp::GROUP_BEGIN: {
                // INTENT: SBE extension rule. A group added in a later schema version
                // is simply absent from messages encoded by an older publisher, so it
                // must be skipped, nested ops included. Found on CME MDP 3.0: a v9
                // schema walking v6 packets read the next message as a group header.
                if (op.since_version > acting_version) { i = static_cast<uint32_t>(op.jump) + 1u; break; }
                if (avail - pos < op.hdr_size) { total_len = 0; return WalkStatus::TRUNCATED; }
                const uint64_t blen = detail::sbe_read_uint(msg + pos + op.dim_block_len_offset, op.dim_block_len_size, little_);
                const uint64_t num  = detail::sbe_read_uint(msg + pos + op.dim_num_offset, op.dim_num_size, little_);
                pos += op.hdr_size;
                if (num == 0) { i = static_cast<uint32_t>(op.jump) + 1u; break; }
                // INTENT: reject num*blen > remaining up front so a corrupt count cannot spin
                // the loop; without it a 32-bit numInGroup would iterate billions of times.
                if (num * blen > static_cast<uint64_t>(avail - pos)) { total_len = 0; return WalkStatus::MALFORMED; }
                if (depth >= SBE_MAX_GROUP_DEPTH) { total_len = 0; return WalkStatus::MALFORMED; }
                if (blen == 0 && op.jump == i + 1u) { i = static_cast<uint32_t>(op.jump) + 1u; break; } // empty entries
                stack[depth++] = Frame{num, blen};
                pos += static_cast<uint32_t>(blen);
                ++i;
                break;
            }
            case WalkOp::GROUP_END: {
                Frame& f = stack[depth - 1];
                if (--f.remaining > 0) {
                    if (avail - pos < f.block_len) { total_len = 0; return WalkStatus::TRUNCATED; }
                    pos += static_cast<uint32_t>(f.block_len);
                    i = static_cast<uint32_t>(op.jump) + 1u;
                } else {
                    --depth;
                    ++i;
                }
                break;
            }
            case WalkOp::VAR_DATA: {
                if (op.since_version > acting_version) { ++i; break; }
                if (avail - pos < op.hdr_size) { total_len = 0; return WalkStatus::TRUNCATED; }
                const uint64_t len = detail::sbe_read_uint(msg + pos + op.dim_num_offset, op.len_prefix_size, little_);
                pos += op.hdr_size;
                if (len > static_cast<uint64_t>(avail - pos)) { total_len = 0; return WalkStatus::TRUNCATED; }
                pos += static_cast<uint32_t>(len);
                ++i;
                break;
            }
            default:
                total_len = 0;
                return WalkStatus::MALFORMED;
        }
    }
    total_len = pos;
    return WalkStatus::OK;
}

} // namespace flashtick
