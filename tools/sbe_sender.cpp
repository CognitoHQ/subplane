// ft_sbe_sender: synthetic bus publisher for tests and benchmarks.
//
// Publishes SBE messages for any schema laid out by a `framing:` block, over
// UDP multicast or unicast. Templates generated: Trade(1), Quote(2),
// BookUpdate(3, 1-3 levels + nested orders + Note), Heartbeat(100),
// SeqReset(101), and an unknown template id 999 on demand.
//
// --count counts datagrams (including injected duplicates).

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <thread>
#include <vector>

#include "packet_gen.h"

#ifndef FT_DEFAULT_SCHEMA
#define FT_DEFAULT_SCHEMA "tests/data/sample_md.xml"
#endif

using namespace flashtick;
using namespace fttool;

namespace {

volatile std::sig_atomic_t g_stop = 0;
void on_sigint(int) { g_stop = 1; }

void usage() {
    std::fputs(
        "usage: ft_sbe_sender --port N [options]\n"
        "  --schema <xml>          SBE schema (default: tests/data/sample_md.xml)\n"
        "  --framing <yaml|file>   framing block (default: bare concatenation)\n"
        "  --group <ip>            multicast group (empty = unicast)\n"
        "  --host <ip>             unicast destination (default 127.0.0.1)\n"
        "  --port <n>              destination port\n"
        "  --interface <ip>        IP_MULTICAST_IF\n"
        "  --rate <msgs/s>         0 = as fast as possible (default 0)\n"
        "  --count <n>             datagrams to send, 0 = forever (default 0)\n"
        "  --symbols <n>           SecurityIDs 1..n (default 10)\n"
        "  --per-packet <n>        messages per datagram (default 1)\n"
        "  --mix trade=70,quote=25,book=5\n"
        "  --heartbeat-ms <n>      Heartbeat(100) every n ms, 0 = off\n"
        "  --gap-every <n>         skip one sequence number every n packets\n"
        "  --seq-reset-at <n>      SeqReset(101) at packet n, sequence restarts at 1\n"
        "  --unknown-every <n>     inject templateId 999 every n packets\n"
        "  --dup-every <n>         resend the previous packet every n packets\n",
        stderr);
}

} // namespace

int main(int argc, char** argv) {
    Args args;
    std::string err;
    if (!args.parse(argc, argv,
                    {"schema", "framing", "group", "host", "port", "interface", "rate", "count", "symbols",
                     "per-packet", "mix", "heartbeat-ms", "gap-every", "seq-reset-at", "unknown-every", "dup-every"},
                    {"help"}, err)) {
        std::fprintf(stderr, "error: %s\n", err.c_str());
        usage();
        return 2;
    }
    if (args.has("help")) { usage(); return 0; }
    GenOptions o;
    o.schema = args.str("schema", o.schema);
    o.framing = args.str("framing", "");
    o.group = args.str("group", "");
    o.host = args.str("host", o.host);
    o.interface = args.str("interface", "");
    if (!args.u64("port", 0, o.port, err) || !args.u64("rate", 0, o.rate, err) || !args.u64("count", 0, o.count, err) ||
        !args.u64("symbols", 10, o.symbols, err) || !args.u64("per-packet", 1, o.per_packet, err) ||
        !args.u64("heartbeat-ms", 0, o.heartbeat_ms, err) || !args.u64("gap-every", 0, o.gap_every, err) ||
        !args.u64("seq-reset-at", 0, o.seq_reset_at, err) || !args.u64("unknown-every", 0, o.unknown_every, err) ||
        !args.u64("dup-every", 0, o.dup_every, err)) {
        std::fprintf(stderr, "error: %s\n", err.c_str());
        return 2;
    }
    if (args.has("mix") && !parse_mix(args.str("mix", ""), o, err)) { std::fprintf(stderr, "error: %s\n", err.c_str()); return 2; }
    if (o.port == 0 || o.port > 65535 || o.symbols == 0 || o.per_packet == 0) {
        std::fprintf(stderr, "error: --port (1-65535), --symbols >= 1 and --per-packet >= 1 are required\n");
        usage();
        return 2;
    }

    LoadedSchema ls;
    FramingConfig cfg;
    if (!ls.load(o.schema, err) || !load_framing(o.framing, cfg, err)) {
        std::fprintf(stderr, "error: %s\n", err.c_str());
        return 2;
    }
    Generator gen(ls.schema, cfg, o);
    if (!gen.init(err)) { std::fprintf(stderr, "error: %s\n", err.c_str()); return 2; }

    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) { std::perror("socket"); return 1; }
    sockaddr_in dst{};
    dst.sin_family = AF_INET;
    dst.sin_port = htons(static_cast<uint16_t>(o.port));
    if (inet_pton(AF_INET, (o.group.empty() ? o.host : o.group).c_str(), &dst.sin_addr) != 1) {
        std::fprintf(stderr, "error: bad destination address\n");
        return 2;
    }
    if (!o.group.empty()) {
        const unsigned char ttl = 1, loop = 1;
        setsockopt(fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));
        setsockopt(fd, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop));
        if (!o.interface.empty()) {
            in_addr ifa{};
            if (inet_pton(AF_INET, o.interface.c_str(), &ifa) != 1 ||
                setsockopt(fd, IPPROTO_IP, IP_MULTICAST_IF, &ifa, sizeof(ifa)) < 0) {
                std::fprintf(stderr, "error: cannot set multicast interface %s\n", o.interface.c_str());
                return 2;
            }
        }
    }
    std::signal(SIGINT, on_sigint);
    std::signal(SIGTERM, on_sigint);

    const uint64_t interval_ns = o.rate ? o.per_packet * 1000000000ULL / o.rate : 0;
    const uint64_t hb_ns = o.heartbeat_ms * 1000000ULL;
    const uint64_t t0 = mono_ns();
    uint64_t deadline = t0, next_hb = t0 + hb_ns, sent = 0, k = 0, dups = 0;

    auto send_pkt = [&](const uint8_t* d, uint32_t len) {
        for (int tries = 0; tries < 100; ++tries) {
            if (::sendto(fd, d, len, 0, reinterpret_cast<const sockaddr*>(&dst), sizeof(dst)) >= 0) return;
            if (errno != EAGAIN && errno != ENOBUFS && errno != EINTR) break;
        }
        ++gen.stats().send_errors;
    };

    while (!g_stop && (o.count == 0 || sent < o.count)) {
        if (interval_ns) {
            deadline += interval_ns;
            for (;;) {
                const uint64_t now = mono_ns();
                if (now >= deadline || g_stop) break;
                if (deadline - now > 100000) std::this_thread::sleep_for(std::chrono::nanoseconds(deadline - now - 50000));
            }
        }
        ++k;
        bool hb_due = false;
        if (hb_ns) {
            const uint64_t now = mono_ns();
            if (now >= next_hb) { hb_due = true; next_hb = now + hb_ns; }
        }
        const uint32_t len = gen.build(k, hb_due);
        if (len == 0) { std::fprintf(stderr, "error: packet exceeds buffer; reduce --per-packet\n"); return 1; }
        send_pkt(gen.data(), len);
        ++sent;
        if (o.dup_every && k % o.dup_every == 0 && (o.count == 0 || sent < o.count)) {
            send_pkt(gen.data(), len);
            ++sent;
            ++dups;
        }
    }
    const double el = static_cast<double>(mono_ns() - t0) / 1e9;
    const GenStats& st = gen.stats();
    std::printf("sent: packets=%llu messages=%llu bytes=%llu elapsed=%.3fs rate=%.0f msg/s (%.0f pkt/s) "
                "gaps_injected=%llu dups_injected=%llu send_errors=%llu\n",
                static_cast<unsigned long long>(sent), static_cast<unsigned long long>(st.messages),
                static_cast<unsigned long long>(st.bytes), el, el > 0 ? static_cast<double>(st.messages) / el : 0.0,
                el > 0 ? static_cast<double>(sent) / el : 0.0, static_cast<unsigned long long>(st.gaps),
                static_cast<unsigned long long>(dups), static_cast<unsigned long long>(st.send_errors));
    ::close(fd);
    return 0;
}
