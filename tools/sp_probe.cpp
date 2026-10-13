// sp_probe: validates a framing block against live packets.
//
// Captures N packets from a multicast group (or unicast port), then runs the
// production Framer + schema walker over them and reports whether the framing
// description is consistent with what is actually on the wire.
//
// Exit code: 0 PASS, 1 FAIL, 2 timeout/usage error with zero packets.

#include <poll.h>

#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "tool_common.h"
#include "subplane/transport/udp_multicast.h"

#ifndef SP_DEFAULT_SCHEMA
#define SP_DEFAULT_SCHEMA "tests/data/sample_md.xml"
#endif

using namespace subplane;
using namespace sptool;

namespace {

volatile std::sig_atomic_t g_stop = 0;
void on_sigint(int) { g_stop = 1; }

void usage() {
    std::fputs(
        "usage: sp_probe --port N [options]\n"
        "  --schema <xml>          SBE schema (default: tests/data/sample_md.xml)\n"
        "  --framing <yaml|file>   framing block to validate (default: try bare concatenation)\n"
        "  --group <ip>            multicast group (empty = unicast bind)\n"
        "  --port <n>              port\n"
        "  --interface <ip>        local interface address\n"
        "  --packets <n>           packets to capture (default 100)\n"
        "  --timeout-ms <n>        give up after this long without packets (default 5000)\n"
        "  --verbose               print every problem as it is found\n"
        "exit: 0 PASS, 1 FAIL, 2 no packets / usage error\n",
        stderr);
}

struct Report {
    std::string name;
    uint64_t packets = 0, messages = 0, bytes = 0;
    uint64_t short_hdr = 0, truncated = 0, malformed = 0, undecodable = 0, trailing = 0, count_mismatch = 0;
    uint64_t length_mismatch = 0, schema_mismatch = 0, unknown_msgs = 0, bad_msg_hdr = 0;
    uint64_t seq_gaps = 0, seq_missing = 0, seq_dups = 0, seq_resets = 0;
    bool seq_checked = false;
    std::vector<uint16_t> unknown_ids;          // first 10 distinct
    std::map<uint16_t, uint64_t> per_template;
    uint64_t min_mpp = UINT64_MAX, max_mpp = 0;
    std::vector<std::string> reasons;

    bool pass() const { return reasons.empty(); }
};

uint64_t read_uint(const uint8_t* p, unsigned size, bool little) { return detail::sbe_read_uint(p, size, little); }

// Sequence tracker: +1 expected; gaps and dups are counted, never failures.
struct SeqTracker {
    bool have = false;
    uint64_t expected = 0;
    void observe(uint64_t seq, Report& r) {
        if (have) {
            if (seq > expected) { ++r.seq_gaps; r.seq_missing += seq - expected; }
            else if (seq < expected) { ++r.seq_dups; return; }
        }
        have = true;
        expected = seq + 1;
    }
};

Report analyze(const std::string& name, const FramingConfig& cfg, const LoadedSchema& ls,
               const std::vector<std::vector<uint8_t>>& pkts, bool verbose, std::string& err) {
    Report r;
    r.name = name;
    Framer fr;
    if (!fr.init(cfg, &ls.walker, err)) { r.reasons.push_back("framing init: " + err); return r; }
    const SbeSchema& sc = ls.schema;
    const HeaderLayout& h = sc.header();
    const bool little = sc.little_endian();
    const bool msg_seq = cfg.seq_source == SeqSource::MESSAGE_FIELD;
    r.seq_checked = cfg.seq_source != SeqSource::NONE;
    SeqTracker seq;

    uint64_t pidx = 0;
    for (const auto& pk : pkts) {
        ++pidx;
        ++r.packets;
        r.bytes += pk.size();
        FrameInfo info;
        MessageIter it;
        const PacketView pv{pk.data(), static_cast<uint32_t>(pk.size()), 0};
        if (!fr.parse(pv, info, it)) {
            ++r.short_hdr;
            if (verbose) std::printf("  pkt %llu: shorter than packet header (%zu < %u)\n",
                                     static_cast<unsigned long long>(pidx), pk.size(), cfg.packet_header_size);
            continue;
        }
        if (info.has_seq) seq.observe(info.seq, r);

        const uint8_t* msg = nullptr;
        uint32_t len = 0;
        uint64_t in_pkt = 0;
        NextStatus st;
        while ((st = fr.next(it, msg, len)) == NextStatus::OK) {
            ++in_pkt;
            ++r.messages;
            if (len < h.size) { ++r.bad_msg_hdr; continue; }
            const auto tid = static_cast<uint16_t>(read_uint(msg + h.template_id_off, h.template_id_size, little));
            if (read_uint(msg + h.schema_id_off, h.schema_id_size, little) != sc.schema_id()) {
                ++r.schema_mismatch;
                if (verbose) std::printf("  pkt %llu: schemaId mismatch on template %u\n", static_cast<unsigned long long>(pidx), tid);
            }
            const MessageDef* def = sc.find_message(tid);
            if (!def) {
                ++r.unknown_msgs;
                if (std::find(r.unknown_ids.begin(), r.unknown_ids.end(), tid) == r.unknown_ids.end() && r.unknown_ids.size() < 10)
                    r.unknown_ids.push_back(tid);
                if (verbose) std::printf("  pkt %llu: unknown templateId %u\n", static_cast<unsigned long long>(pidx), tid);
                continue;
            }
            ++r.per_template[tid];
            if (cfg.len_source == LenSource::MESSAGE_PREFIX) {
                // The strongest framing check: the schema must agree with the declared length.
                uint32_t walked = 0;
                const WalkStatus ws = ls.walker.walk(msg, len, walked);
                if (ws != WalkStatus::OK || walked != len) {
                    ++r.length_mismatch;
                    if (verbose) std::printf("  pkt %llu: template %u declared length %u, schema walk %u (status %d)\n",
                                             static_cast<unsigned long long>(pidx), tid, len, walked, static_cast<int>(ws));
                }
            }
            if (msg_seq) {
                const FieldDef* f = sc.find_field(*def, cfg.seq_field);
                if (f && static_cast<uint32_t>(h.size + f->offset + f->size) <= len)
                    seq.observe(read_uint(msg + h.size + f->offset, f->size, little), r);
            }
            if (tid == 101) {
                // SeqReset: sequence restarts at NewSeq.
                const FieldDef* f = sc.find_field(*def, "NewSeq");
                if (f && static_cast<uint32_t>(h.size + f->offset + f->size) <= len) {
                    seq.have = true;
                    seq.expected = read_uint(msg + h.size + f->offset, f->size, little);
                    ++r.seq_resets;
                }
            }
        }
        switch (st) {
            case NextStatus::END:
                if (it.cur < it.end) {
                    ++r.trailing; // only reachable when `count` bounded the loop
                    if (verbose) std::printf("  pkt %llu: %td trailing bytes after %u messages\n",
                                             static_cast<unsigned long long>(pidx), it.end - it.cur, it.index);
                }
                if (info.has_count && info.count != it.index) {
                    ++r.count_mismatch;
                    if (verbose) std::printf("  pkt %llu: count field %u but %u messages fit\n",
                                             static_cast<unsigned long long>(pidx), info.count, it.index);
                }
                break;
            case NextStatus::TRUNCATED: ++r.truncated;
                if (verbose) std::printf("  pkt %llu: truncated message after %u messages\n", static_cast<unsigned long long>(pidx), it.index);
                break;
            case NextStatus::UNDECODABLE: ++r.undecodable;
                if (verbose) std::printf("  pkt %llu: undecodable tail after %u messages\n", static_cast<unsigned long long>(pidx), it.index);
                break;
            case NextStatus::MALFORMED: ++r.malformed;
                if (verbose) std::printf("  pkt %llu: malformed message after %u messages\n", static_cast<unsigned long long>(pidx), it.index);
                break;
            case NextStatus::OK: break;
        }
        r.min_mpp = std::min(r.min_mpp, in_pkt);
        r.max_mpp = std::max(r.max_mpp, in_pkt);
    }

    auto fail = [&](uint64_t n, const char* what) {
        if (n) r.reasons.push_back(std::to_string(n) + " " + what);
    };
    fail(r.short_hdr, "packet(s) shorter than the packet header");
    fail(r.unknown_msgs, "message(s) with a templateId not in the schema");
    fail(r.length_mismatch, "message(s) whose declared length disagrees with the schema walk (framing block is likely wrong)");
    fail(r.truncated, "truncated packet(s)");
    fail(r.malformed, "malformed message(s)");
    fail(r.undecodable, "packet(s) with an undecodable tail (unknown template, no length prefix)");
    fail(r.count_mismatch, "packet(s) where the count field disagrees with the messages found");
    fail(r.trailing, "packet(s) with trailing bytes");
    fail(r.schema_mismatch, "message(s) with a schemaId different from the schema");
    fail(r.bad_msg_hdr, "message(s) shorter than the SBE header");
    return r;
}

void print_report(const Report& r, const SbeSchema& sc) {
    std::printf("\n== candidate: %s ==\n", r.name.c_str());
    std::printf("verdict:               %s\n", r.pass() ? "PASS" : "FAIL");
    for (const auto& why : r.reasons) std::printf("  reason: %s\n", why.c_str());
    std::printf("packets:               %llu\n", static_cast<unsigned long long>(r.packets));
    std::printf("messages:              %llu\n", static_cast<unsigned long long>(r.messages));
    std::printf("bytes:                 %llu\n", static_cast<unsigned long long>(r.bytes));
    for (const auto& [tid, n] : r.per_template) {
        const MessageDef* d = sc.find_message(tid);
        std::printf("  template %-14s %llu\n", (std::string(d ? d->name : "?") + "(" + std::to_string(tid) + ")").c_str(),
                    static_cast<unsigned long long>(n));
    }
    std::printf("unknown template ids:  ");
    if (r.unknown_ids.empty()) std::printf("none");
    for (uint16_t id : r.unknown_ids) std::printf("%u ", id);
    std::printf("(%llu messages)\n", static_cast<unsigned long long>(r.unknown_msgs));
    std::printf("length mismatches:     %llu\n", static_cast<unsigned long long>(r.length_mismatch));
    std::printf("trailing-byte packets: %llu\n", static_cast<unsigned long long>(r.trailing));
    std::printf("truncated packets:     %llu\n", static_cast<unsigned long long>(r.truncated + r.short_hdr));
    if (r.seq_checked)
        std::printf("sequence gaps: %llu (missing %llu)\nsequence dups: %llu\nsequence resets: %llu\n",
                    static_cast<unsigned long long>(r.seq_gaps), static_cast<unsigned long long>(r.seq_missing),
                    static_cast<unsigned long long>(r.seq_dups), static_cast<unsigned long long>(r.seq_resets));
    else
        std::printf("sequence:              not available in this framing\n");
    if (r.packets > 0 && r.min_mpp != UINT64_MAX)
        std::printf("msgs/packet min/avg/max: %llu / %.2f / %llu\n", static_cast<unsigned long long>(r.min_mpp),
                    static_cast<double>(r.messages) / static_cast<double>(r.packets), static_cast<unsigned long long>(r.max_mpp));
}

} // namespace

int main(int argc, char** argv) {
    Args args;
    std::string err;
    if (!args.parse(argc, argv, {"schema", "framing", "group", "port", "interface", "packets", "timeout-ms"},
                    {"verbose", "help"}, err)) {
        std::fprintf(stderr, "error: %s\n", err.c_str());
        usage();
        return 2;
    }
    if (args.has("help")) { usage(); return 0; }
    uint64_t port = 0, npk = 100, timeout_ms = 5000;
    if (!args.u64("port", 0, port, err) || !args.u64("packets", 100, npk, err) || !args.u64("timeout-ms", 5000, timeout_ms, err) ||
        port == 0 || port > 65535 || npk == 0) {
        std::fprintf(stderr, "error: %s\n", err.empty() ? "--port (1-65535) is required, --packets >= 1" : err.c_str());
        usage();
        return 2;
    }
    const bool verbose = args.has("verbose");

    LoadedSchema ls;
    FramingConfig user_cfg, bare_cfg;
    const bool have_user = args.has("framing");
    if (!ls.load(args.str("schema", SP_DEFAULT_SCHEMA), err) ||
        (have_user && !load_framing(args.str("framing", ""), user_cfg, err)) ||
        !load_framing("", bare_cfg, err)) {
        std::fprintf(stderr, "error: %s\n", err.c_str());
        return 2;
    }

    UdpMulticastConfig uc;
    uc.group = args.str("group", "");
    uc.port = static_cast<uint16_t>(port);
    uc.interface = args.str("interface", "");
    uc.mtu = 9216;
    UdpMulticastTransport tr(uc);
    if (!tr.open(err)) { std::fprintf(stderr, "error: open %s: %s\n", tr.describe().c_str(), err.c_str()); return 2; }
    std::signal(SIGINT, on_sigint);
    std::fprintf(stderr, "sp_probe: listening on %s, capturing up to %llu packets\n", tr.describe().c_str(),
                 static_cast<unsigned long long>(npk));

    std::vector<std::vector<uint8_t>> pkts;
    pkts.reserve(npk);
    PacketBatch batch;
    auto last_rx = std::chrono::steady_clock::now();
    while (!g_stop && pkts.size() < npk) {
        const int n = tr.poll(batch);
        if (n < 0) { std::fprintf(stderr, "error: socket failure\n"); break; }
        for (int i = 0; i < n && pkts.size() < npk; ++i) pkts.emplace_back(batch.pkts[i].data, batch.pkts[i].data + batch.pkts[i].len);
        if (n > 0) { last_rx = std::chrono::steady_clock::now(); continue; }
        const auto idle = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - last_rx).count();
        if (static_cast<uint64_t>(idle) >= timeout_ms) break;
        pollfd pfd{tr.fd(), POLLIN, 0};
        ::poll(&pfd, 1, 50);
    }
    tr.close();

    if (pkts.empty()) {
        std::printf("verdict: NO DATA (no packets within %llu ms)\n", static_cast<unsigned long long>(timeout_ms));
        return 2;
    }
    if (pkts.size() < npk) std::fprintf(stderr, "sp_probe: only %zu of %llu packets captured\n", pkts.size(), static_cast<unsigned long long>(npk));

    Report primary = analyze(have_user ? "user framing" : "bare concatenation", have_user ? user_cfg : bare_cfg, ls, pkts, verbose, err);
    std::printf("%s\n", primary.pass() ? "PASS" : "FAIL");
    print_report(primary, ls.schema);
    if (have_user) {
        std::string e2;
        Report bare = analyze("bare concatenation", bare_cfg, ls, pkts, false, e2);
        std::printf("\nalso tried bare concatenation: %s\n", bare.pass() ? "PASS" : "FAIL");
    }
    return primary.pass() ? 0 : 1;
}
