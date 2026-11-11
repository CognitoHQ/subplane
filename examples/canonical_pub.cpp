// SPDX-License-Identifier: Apache-2.0
// canonical_pub: the smallest publisher that subplane can ingest. Copy this to
// start a gateway: it depends on nothing but the generated canonical struct
// header and sockets. No agent code, no SDK.
//
//   canonical_pub --port N [--host 127.0.0.1 | --group 239.1.1.1] [--rate msg/s] [--count N] [--symbols N]
//
// Wire format = the framing block the agent is configured with (config.yaml,
// partition `canonical`): packet header {seq u64le, count u16le}, then per
// message {len u16le}{SBE message}. The SBE message is the generated struct,
// filled in place: header fields first, then the canonical prefix, then the body.
// Prices and quantities are int64 scaled by 1e-9.

#include "subplane_canonical_views.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

using namespace sp_canonical;

namespace {

uint64_t now_ns() { return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count()); }

// Every message starts with the SBE header and the 40-byte canonical prefix.
// `seq` counts the stream, `iseq` this instrument.
template <class T>
void begin(T& m, uint32_t instrument, uint64_t seq, uint64_t iseq) {
    std::memset(&m, 0, sizeof m);
    m.blockLength = static_cast<uint16_t>(T::kSize - kHeaderSize);
    m.templateId = T::kTemplateId;
    m.schemaId = kSchemaId;
    m.version = kSchemaVersion;
    m.InstrumentId = instrument;
    m.VenueId = 1;
    m.Sequence = seq;
    m.InstrumentSeq = iseq;
    m.ExchangeTs = now_ns();
    m.RecvTs = m.ExchangeTs;
}

// Appends one framed message: {len u16le}{bytes}.
template <class T>
void append(uint8_t* pkt, uint32_t& pos, const T& m) {
    const uint16_t len = static_cast<uint16_t>(sizeof m);
    std::memcpy(pkt + pos, &len, 2);
    std::memcpy(pkt + pos + 2, &m, sizeof m);
    pos += 2 + sizeof m;
}

} // namespace

int main(int argc, char** argv) {
    std::string host = "127.0.0.1", group;
    uint64_t port = 0, rate = 1000, count = 0, symbols = 5;
    for (int i = 1; i + 1 < argc; i += 2) {
        const std::string k = argv[i], v = argv[i + 1];
        if (k == "--port") port = std::strtoull(v.c_str(), nullptr, 10);
        else if (k == "--host") host = v;
        else if (k == "--group") group = v;
        else if (k == "--rate") rate = std::strtoull(v.c_str(), nullptr, 10);
        else if (k == "--count") count = std::strtoull(v.c_str(), nullptr, 10);
        else if (k == "--symbols") symbols = std::strtoull(v.c_str(), nullptr, 10);
        else { std::fprintf(stderr, "canonical_pub: unknown option %s\n", k.c_str()); return 2; }
    }
    if (port == 0 || port > 65535 || symbols == 0) {
        std::fputs("usage: canonical_pub --port N [--host 127.0.0.1 | --group 239.1.1.1] [--rate msg/s] [--count N] [--symbols N]\n", stderr);
        return 2;
    }
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) { std::perror("socket"); return 1; }
    sockaddr_in dst{};
    dst.sin_family = AF_INET;
    dst.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, (group.empty() ? host : group).c_str(), &dst.sin_addr) != 1) { std::fputs("bad address\n", stderr); return 2; }
    if (!group.empty()) { const unsigned char ttl = 1, loop = 1; setsockopt(fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, 1); setsockopt(fd, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, 1); }

    uint8_t pkt[1400];
    uint64_t pkt_seq = 1, msg_seq = 0, sent = 0;
    std::vector<uint64_t> iseq(symbols + 1, 0);
    const auto interval = std::chrono::nanoseconds(rate ? 2'000'000'000ull / rate : 0); // two messages per packet
    auto next = std::chrono::steady_clock::now();
    while (count == 0 || sent < count) {
        // Packet header: seq u64le, count u16le.
        uint32_t pos = 0;
        std::memcpy(pkt, &pkt_seq, 8);
        const uint16_t n = 2;
        std::memcpy(pkt + 8, &n, 2);
        pos = 10;

        const uint32_t sym = static_cast<uint32_t>(1 + msg_seq % symbols);
        const int64_t px = 100'000'000'000ll + static_cast<int64_t>(sym) * 1'000'000'000ll + static_cast<int64_t>(msg_seq % 50) * 10'000'000ll; // 100.xx .. in 1e-9 units
        Trade t; begin(t, sym, ++msg_seq, ++iseq[sym]);
        t.Price = px; t.Qty = 250'000'000; t.TradeId = msg_seq; t.Aggressor = static_cast<uint8_t>(1 + msg_seq % 2);
        append(pkt, pos, t);
        Quote q; begin(q, sym, ++msg_seq, ++iseq[sym]);
        q.BidPx = px - 10'000'000; q.BidQty = 1'000'000'000; q.AskPx = px + 10'000'000; q.AskQty = 2'000'000'000;
        append(pkt, pos, q);

        if (::sendto(fd, pkt, pos, 0, reinterpret_cast<const sockaddr*>(&dst), sizeof dst) < 0) { std::perror("sendto"); return 1; }
        ++pkt_seq;
        sent += n;
        if (interval.count()) { next += interval; std::this_thread::sleep_until(next); }
    }
    std::printf("sent %llu messages in %llu packets\n", static_cast<unsigned long long>(sent), static_cast<unsigned long long>(pkt_seq - 1));
    ::close(fd);
    return 0;
}
