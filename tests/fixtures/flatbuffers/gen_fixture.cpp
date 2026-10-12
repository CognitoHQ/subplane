// Fixture generator for the flatbuffers decoder tests. Built by hand, not by
// CMake, so the agent build never depends on FlatBuffers:
//   /usr/bin/flatc --cpp md.fbs
//   g++ -std=c++20 -O2 -Wall gen_fixture.cpp -o gen_fixture && ./gen_fixture
// Writes fb_sample.bin (N size-prefixed buffers back to back), fb_sample.pcap
// (the same buffers as UDP/IPv4/Ethernet datagrams, 1..4 per datagram) and
// fb_sample.expected (kind seq ts instrument venue symbol flags per message).
// Every 5th message is an "old producer": `flags` never set, so its vtable is
// shorter than the current schema's. The committed outputs were produced with
// flatc 2.0.8 (see README.md).
#include "md_generated.h"
#include <cstdio>
#include <cstring>
#include <random>
#include <vector>
using namespace md;

static void put16be(std::vector<uint8_t>& v, uint16_t x) { v.push_back(uint8_t(x >> 8)); v.push_back(uint8_t(x)); }
static void put32le(std::vector<uint8_t>& v, uint32_t x) { for (int i = 0; i < 4; ++i) v.push_back(uint8_t(x >> (8 * i))); }

int main(int argc, char** argv) {
    const int N = argc > 1 ? atoi(argv[1]) : 300;
    std::mt19937_64 rng(42);
    std::vector<std::vector<uint8_t>> msgs;
    FILE* exp = fopen("fb_sample.expected", "w");
    flatbuffers::FlatBufferBuilder fbb(1024);
    for (int i = 0; i < N; ++i) {
        fbb.Clear();
        const uint32_t iid = 1000 + uint32_t(rng() % 50);
        const uint16_t venue = uint16_t(1 + rng() % 3);
        char sym[16]; snprintf(sym, sizeof sym, "SYM%u", iid);
        const uint64_t ts = 1758742200000000000ull + uint64_t(i) * 1000;
        const int kind = i % 10 == 9 ? 3 : (i % 2 ? 1 : 2); // 1 trade, 2 quote, 3 heartbeat
        flatbuffers::Offset<void> body; Body bt;
        if (kind == 3) { body = CreateHeartbeat(fbb, fbb.CreateString("hb")).Union(); bt = Body_Heartbeat; }
        else {
            auto ins = CreateInstrument(fbb, iid, venue, fbb.CreateString(sym));
            if (kind == 1) { body = CreateTrade(fbb, ins, 1234500 + i, 10, 'B').Union(); bt = Body_Trade; }
            else { body = CreateQuote(fbb, ins, 1234400, 1234600, 5, 7).Union(); bt = Body_Quote; }
        }
        const bool old = i % 5 == 4;
        EnvelopeBuilder eb(fbb);
        eb.add_seq(uint64_t(i) + 1); eb.add_ts(ts); eb.add_body_type(bt); eb.add_body(body);
        if (!old) eb.add_flags(7);
        fbb.FinishSizePrefixed(eb.Finish(), EnvelopeIdentifier());
        msgs.emplace_back(fbb.GetBufferPointer(), fbb.GetBufferPointer() + fbb.GetSize());
        fprintf(exp, "%d %llu %llu %u %u %s %d\n", kind, (unsigned long long)(i + 1), (unsigned long long)ts,
                kind == 3 ? 0u : iid, kind == 3 ? 0u : venue, kind == 3 ? "-" : sym, old ? 0 : 7);
    }
    fclose(exp);

    FILE* bin = fopen("fb_sample.bin", "wb");
    size_t bytes = 0;
    for (const auto& m : msgs) { fwrite(m.data(), 1, m.size(), bin); bytes += m.size(); }
    fclose(bin);

    // pcap: global header, then Ethernet/IPv4/UDP datagrams holding 1..4 buffers each.
    std::vector<uint8_t> pc;
    put32le(pc, 0xa1b2c3d4); pc.push_back(2); pc.push_back(0); pc.push_back(4); pc.push_back(0);
    put32le(pc, 0); put32le(pc, 0); put32le(pc, 65535); put32le(pc, 1);
    size_t i = 0; int datagrams = 0;
    while (i < msgs.size()) {
        const size_t per = (datagrams % 4) + 1;
        std::vector<uint8_t> payload;
        for (size_t k = 0; k < per && i < msgs.size(); ++k, ++i) payload.insert(payload.end(), msgs[i].begin(), msgs[i].end());
        std::vector<uint8_t> frame;
        for (int k = 0; k < 6; ++k) frame.push_back(0x02);            // dst mac
        for (int k = 0; k < 6; ++k) frame.push_back(0x04);            // src mac
        put16be(frame, 0x0800);
        const uint16_t iplen = uint16_t(20 + 8 + payload.size());
        frame.push_back(0x45); frame.push_back(0); put16be(frame, iplen);
        put16be(frame, uint16_t(datagrams)); put16be(frame, 0x4000);  // id, DF
        frame.push_back(64); frame.push_back(17); put16be(frame, 0);  // ttl, udp, checksum 0
        for (uint8_t b : {10, 0, 0, 1}) frame.push_back(b);           // src
        for (uint8_t b : {10, 0, 0, 2}) frame.push_back(b);           // dst
        put16be(frame, 40000); put16be(frame, 24000); put16be(frame, uint16_t(8 + payload.size())); put16be(frame, 0);
        frame.insert(frame.end(), payload.begin(), payload.end());
        put32le(pc, 1758742200u + uint32_t(datagrams)); put32le(pc, uint32_t(datagrams) * 100u);
        put32le(pc, uint32_t(frame.size())); put32le(pc, uint32_t(frame.size()));
        pc.insert(pc.end(), frame.begin(), frame.end());
        ++datagrams;
    }
    FILE* pf = fopen("fb_sample.pcap", "wb"); fwrite(pc.data(), 1, pc.size(), pf); fclose(pf);
    printf("%d messages, %zu bytes, %d datagrams\n", N, bytes, datagrams);
}
