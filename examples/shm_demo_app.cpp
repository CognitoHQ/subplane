// SPDX-License-Identifier: Apache-2.0
// shm_demo_app: the complete app-side contract in one file.
//
//   shm_demo_app --socket /run/subplane/subplane.sock [--session demo]
//                [--symbols 1,2,3] [--trades-only] [--seconds N] [--quiet]
//
// 1. connect to the agent's control socket and say hello
// 2. create (or attach to) a session with a filter
// 3. open the session's shared-memory ring
// 4. mark the live head, fetch the last-value snapshot, seek back to the mark
// 5. tail the ring, deduplicating the overlap by sequence, handling in-band
//    control events, and re-snapshotting on overrun
//
// Apps decode the SBE payload with their own generated codecs; here we only
// print the routing header. No part of this file depends on anything but the
// two client headers.

#include "subplane/client/control_client.h"
#include "subplane/client/shm_reader.h"
#include "subplane/core/clock.h"
#include "subplane/shm/control_events.h"

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace subplane;

namespace {

volatile sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

struct Book {
    // Per-symbol last state an app would maintain. Here: last sequence + counts.
    std::unordered_map<uint32_t, uint64_t> last_seq;
    uint64_t data = 0, snapshot = 0, control = 0, dedup = 0, lapped = 0;
};

void apply(Book& b, const TickHeader& h, bool from_snapshot, bool quiet) {
    if (from_snapshot) ++b.snapshot; else ++b.data;
    b.last_seq[h.symbol_id] = h.sequence;
    if (!quiet) {
        std::printf("%s sym=%u type=%u seq=%llu exch_ts=%llu ingest=%llu len=%u\n",
                    from_snapshot ? "SNAP" : "DATA", h.symbol_id, h.msg_type,
                    static_cast<unsigned long long>(h.sequence), static_cast<unsigned long long>(h.exchange_ts),
                    static_cast<unsigned long long>(h.ingest_ts), h.payload_len);
    }
}

void on_control(Book& b, const TickHeader& h, const uint8_t* p, uint32_t len, bool quiet) {
    ++b.control;
    if (quiet) return;
    switch (static_cast<ControlCode>(h.msg_type)) {
        case ControlCode::GAP_DETECTED: {
            GapEvent e; if (len >= sizeof(e)) { std::memcpy(&e, p, sizeof(e));
                std::printf("CTRL gap topic=%u expected=%llu received=%llu\n", e.topic_id,
                            static_cast<unsigned long long>(e.expected_seq), static_cast<unsigned long long>(e.received_seq)); }
            break;
        }
        case ControlCode::SYMBOL_MAPPED: {
            SymbolMappedEvent e; if (len >= sizeof(e)) { std::memcpy(&e, p, sizeof(e));
                std::printf("CTRL symbol id=%u name=%.*s\n", e.symbol_id, static_cast<int>(e.name_len), e.name); }
            break;
        }
        case ControlCode::FILTER_RELOADED:    std::printf("CTRL filter reloaded\n"); break;
        case ControlCode::TOPIC_DEGRADED:     std::printf("CTRL topic degraded\n"); break;
        case ControlCode::TOPIC_RECOVERED:    std::printf("CTRL topic recovered\n"); break;
        case ControlCode::AGENT_RESTARTED:    std::printf("CTRL agent restarted\n"); break;
        case ControlCode::SESSION_CLOSING:    std::printf("CTRL session closing\n"); break;
        case ControlCode::TRANSPORT_DOWN:
        case ControlCode::TRANSPORT_UP: {
            TransportStateEvent e; if (len >= sizeof(e)) { std::memcpy(&e, p, sizeof(e));
                std::printf("CTRL transport %s topic=%u reason=%u\n",
                            h.msg_type == static_cast<uint8_t>(ControlCode::TRANSPORT_UP) ? "up" : "down", e.topic_id, e.reason); }
            break;
        }
        case ControlCode::BUS_HEARTBEAT:      break;
        default: std::printf("CTRL code=0x%02x topic=%u\n", h.msg_type, h.topic_id); break;
    }
}

} // namespace

int main(int argc, char** argv) {
    std::string socket_path = "/run/subplane/subplane.sock";
    std::string session = "demo";
    std::vector<std::string> symbols;
    bool trades_only = false, quiet = false;
    int seconds = 0;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--socket" && i + 1 < argc) socket_path = argv[++i];
        else if (a == "--session" && i + 1 < argc) session = argv[++i];
        else if (a == "--symbols" && i + 1 < argc) {
            std::stringstream ss(argv[++i]);
            std::string s;
            while (std::getline(ss, s, ',')) if (!s.empty()) symbols.push_back(s);
        }
        else if (a == "--trades-only") trades_only = true;
        else if (a == "--seconds" && i + 1 < argc) seconds = std::atoi(argv[++i]);
        else if (a == "--quiet") quiet = true;
        else { std::fprintf(stderr, "unknown option %s\n", a.c_str()); return 2; }
    }
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    ControlClient ctl;
    HelloInfo hi;
    if (!ctl.connect(socket_path) || !ctl.hello(hi)) {
        std::fprintf(stderr, "control: %s\n", ctl.last_error().c_str());
        return 1;
    }
    std::printf("# agent=%s epoch=%llu record_version=%u partitions=%zu symbols=%u\n", hi.agent_name.c_str(),
                static_cast<unsigned long long>(hi.epoch), hi.record_version, hi.topics.size(), hi.symbol_count);

    SessionRequest req;
    req.name = session;
    req.ring_capacity_bytes = 8ull << 20;
    req.max_payload = 4096;
    if (trades_only) req.msg_type_mask = 1ull << static_cast<unsigned>(MsgType::TRADE);
    req.all_symbols = symbols.empty();
    req.symbols = symbols;

    AttachInfo ai;
    if (!ctl.create_session(req, ai) && !ctl.attach(session, ai)) {
        std::fprintf(stderr, "session: %s\n", ctl.last_error().c_str());
        return 1;
    }
    std::printf("# session=%s owner=%s ring=%s epoch=%llu\n", ai.name.c_str(), ai.owner.c_str(), ai.ring_path.c_str(),
                static_cast<unsigned long long>(ai.epoch));

    ShmReader rd;
    std::string err;
    if (!rd.open(ai.ring_path, &err) || rd.epoch() != hi.epoch) {
        std::fprintf(stderr, "ring: %s\n", err.c_str());
        return 1;
    }

    Book book;
    auto resnapshot = [&]() {
        const uint64_t mark = rd.head();
        uint64_t snap_head = 0;
        if (!ctl.snapshot(session, snap_head, [&](const TickHeader& h, const uint8_t*, uint32_t) {
                apply(book, h, true, quiet);
            })) {
            std::fprintf(stderr, "snapshot: %s\n", ctl.last_error().c_str());
            return false;
        }
        rd.seek(mark);
        return true;
    };
    if (!resnapshot()) return 1;
    std::printf("# snapshot records=%llu; tailing\n", static_cast<unsigned long long>(book.snapshot));

    const uint64_t deadline = seconds > 0 ? now_ns() + static_cast<uint64_t>(seconds) * static_cast<uint64_t>(1'000'000'000) : 0;
    RecordView rv;
    while (!g_stop && (deadline == 0 || now_ns() < deadline)) {
        const ReaderStatus st = rd.poll(rv);
        if (st == ReaderStatus::Ok) {
            if (is_data(*rv.hdr)) {
                uint64_t& s = book.last_seq[rv.hdr->symbol_id];
                if (rv.hdr->sequence != 0 && rv.hdr->sequence <= s) { ++book.dedup; continue; }
                apply(book, *rv.hdr, false, quiet);
                if (!rv.valid()) { ++book.lapped; resnapshot(); }
            } else {
                on_control(book, *rv.hdr, rv.payload, rv.payload_len, quiet);
                if (rv.hdr->msg_type == static_cast<uint8_t>(ControlCode::SESSION_CLOSING)) break;
            }
            continue;
        }
        if (st == ReaderStatus::Overrun) { std::printf("# overrun: resnapshot\n"); resnapshot(); continue; }
        if (st == ReaderStatus::Closed) { std::printf("# ring closed\n"); break; }
        if (!rd.writer_alive(2'000'000'000ull, now_ns())) { std::printf("# writer gone\n"); break; }
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    std::printf("# summary data=%llu snapshot=%llu control=%llu dedup=%llu lapped=%llu symbols=%zu lag=%llu overruns=%llu\n",
                static_cast<unsigned long long>(book.data), static_cast<unsigned long long>(book.snapshot),
                static_cast<unsigned long long>(book.control), static_cast<unsigned long long>(book.dedup),
                static_cast<unsigned long long>(book.lapped), book.last_seq.size(),
                static_cast<unsigned long long>(rd.lag()), static_cast<unsigned long long>(rd.overruns()));
    ctl.destroy_session(session);
    return 0;
}
