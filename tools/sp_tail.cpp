// sp_tail: dump records from a subplane shared-memory ring for operators.
//
//   sp_tail <ring path> [--count N] [--follow] [--from-start] [--hex]
//
// By default starts at the live head and prints records as they arrive until
// N records (default: until Ctrl-C with --follow, else drain and exit).

#include "subplane/client/shm_reader.h"
#include "subplane/core/clock.h"
#include "subplane/shm/control_events.h"

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

using namespace subplane;

namespace {

volatile sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

const char* code_name(const TickHeader& h) {
    if (h.kind == static_cast<uint8_t>(Kind::DATA)) {
        switch (static_cast<MsgType>(h.msg_type)) {
            case MsgType::TRADE: return "TRADE";
            case MsgType::QUOTE: return "QUOTE";
            case MsgType::BOOK_UPDATE: return "BOOK_UPDATE";
            case MsgType::BOOK_SNAPSHOT: return "BOOK_SNAPSHOT";
            case MsgType::OHLCV: return "OHLCV";
            case MsgType::STATUS: return "STATUS";
            default: return "DATA?";
        }
    }
    switch (static_cast<ControlCode>(h.msg_type)) {
        case ControlCode::BUS_HEARTBEAT: return "BUS_HEARTBEAT";
        case ControlCode::BUS_SEQ_RESET: return "BUS_SEQ_RESET";
        case ControlCode::BUS_SNAPSHOT_START: return "BUS_SNAPSHOT_START";
        case ControlCode::BUS_SNAPSHOT_END: return "BUS_SNAPSHOT_END";
        case ControlCode::BUS_TRADING_STATUS: return "BUS_TRADING_STATUS";
        case ControlCode::GAP_DETECTED: return "GAP_DETECTED";
        case ControlCode::PARTITION_DEGRADED: return "PARTITION_DEGRADED";
        case ControlCode::PARTITION_RECOVERED: return "PARTITION_RECOVERED";
        case ControlCode::FILTER_RELOADED: return "FILTER_RELOADED";
        case ControlCode::AGENT_RESTARTED: return "AGENT_RESTARTED";
        case ControlCode::SESSION_CLOSING: return "SESSION_CLOSING";
        case ControlCode::SYMBOL_MAPPED: return "SYMBOL_MAPPED";
        case ControlCode::PAD: return "PAD";
        default: return "CONTROL?";
    }
}

void print_control_detail(const TickHeader& h, const uint8_t* p, uint32_t len) {
    switch (static_cast<ControlCode>(h.msg_type)) {
        case ControlCode::GAP_DETECTED: {
            if (len < sizeof(GapEvent)) return;
            GapEvent e; std::memcpy(&e, p, sizeof(e));
            std::printf(" partition=%u expected=%llu received=%llu", e.partition_id,
                        static_cast<unsigned long long>(e.expected_seq), static_cast<unsigned long long>(e.received_seq));
            break;
        }
        case ControlCode::SYMBOL_MAPPED: {
            if (len < sizeof(SymbolMappedEvent)) return;
            SymbolMappedEvent e; std::memcpy(&e, p, sizeof(e));
            std::printf(" id=%u name=%.*s", e.symbol_id, static_cast<int>(e.name_len), e.name);
            break;
        }
        case ControlCode::AGENT_RESTARTED: {
            if (len < sizeof(AgentRestartedEvent)) return;
            AgentRestartedEvent e; std::memcpy(&e, p, sizeof(e));
            std::printf(" epoch=%llu", static_cast<unsigned long long>(e.epoch));
            break;
        }
        case ControlCode::FILTER_RELOADED: {
            if (len < sizeof(FilterReloadedEvent)) return;
            FilterReloadedEvent e; std::memcpy(&e, p, sizeof(e));
            std::printf(" session=%u generation=%u", e.session_id, e.generation);
            break;
        }
        case ControlCode::PARTITION_DEGRADED:
        case ControlCode::PARTITION_RECOVERED: {
            if (len < sizeof(PartitionStateEvent)) return;
            PartitionStateEvent e; std::memcpy(&e, p, sizeof(e));
            std::printf(" partition=%u reason=%u last_seq=%llu", e.partition_id, e.reason,
                        static_cast<unsigned long long>(e.last_seq));
            break;
        }
        default: break;
    }
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: sp_tail <ring path> [--count N] [--follow] [--from-start] [--hex]\n");
        return 2;
    }
    std::string path = argv[1];
    long count = -1;
    bool follow = false, from_start = false, hex = false;
    for (int i = 2; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--count" && i + 1 < argc) count = std::atol(argv[++i]);
        else if (a == "--follow") follow = true;
        else if (a == "--from-start") from_start = true;
        else if (a == "--hex") hex = true;
        else { std::fprintf(stderr, "unknown option %s\n", a.c_str()); return 2; }
    }
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    ShmReader rd;
    std::string err;
    if (!rd.open(path, &err)) {
        std::fprintf(stderr, "open %s: %s\n", path.c_str(), err.c_str());
        return 1;
    }
    const RingHeader& rh = rd.header();
    std::printf("# ring %s epoch=%llu capacity=%llu max_payload=%u state=%u head=%llu\n", path.c_str(),
                static_cast<unsigned long long>(rh.epoch), static_cast<unsigned long long>(rh.capacity),
                rh.max_payload, rd.state(), static_cast<unsigned long long>(rd.head()));
    if (from_start) {
        // Position 0 is only still intact before the first wrap; past that the
        // reader detects the overrun on the first poll and resyncs to head.
        rd.seek(0);
    }

    long printed = 0;
    RecordView v;
    while (!g_stop && (count < 0 || printed < count)) {
        const ReaderStatus st = rd.poll(v);
        if (st == ReaderStatus::Ok) {
            const TickHeader& h = *v.hdr;
            std::printf("%llu %s kind=%u sym=%u seq=%llu part=%u tmpl=%u len=%u ingest=%llu",
                        static_cast<unsigned long long>(v.pos), code_name(h), h.kind, h.symbol_id,
                        static_cast<unsigned long long>(h.sequence), h.partition_id, h.template_id,
                        v.payload_len, static_cast<unsigned long long>(h.ingest_ts));
            if (h.kind == static_cast<uint8_t>(Kind::AGENT_CONTROL)) print_control_detail(h, v.payload, v.payload_len);
            if (hex) {
                std::printf(" hex=");
                for (uint32_t i = 0; i < v.payload_len; ++i) std::printf("%02x", v.payload[i]);
            }
            if (!v.valid()) std::printf(" [LAPPED]");
            std::printf("\n");
            ++printed;
            continue;
        }
        if (st == ReaderStatus::Overrun) {
            std::printf("# overrun: resynced to head (%llu total)\n", static_cast<unsigned long long>(rd.overruns()));
            continue;
        }
        if (st == ReaderStatus::Closed) {
            std::printf("# ring closed by writer\n");
            break;
        }
        if (!follow) break;
        if (!rd.writer_alive(2'000'000'000ull, now_ns())) {
            std::printf("# writer gone (no heartbeat)\n");
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    std::fflush(stdout);
    return 0;
}
