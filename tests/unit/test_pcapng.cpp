#include <doctest.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "subplane/record/pcapng.h"
#include "fb_fixture.h"

using namespace subplane::record;

namespace {

// Writes through open_memstream so the writer's FILE* path is the one under test.
struct MemFile {
    char* buf = nullptr;
    size_t len = 0;
    std::FILE* f = open_memstream(&buf, &len);
    std::vector<uint8_t> close() {
        std::fclose(f);
        f = nullptr;
        std::vector<uint8_t> out(reinterpret_cast<uint8_t*>(buf), reinterpret_cast<uint8_t*>(buf) + len);
        std::free(buf);
        buf = nullptr;
        return out;
    }
    ~MemFile() { if (f) std::fclose(f); std::free(buf); }
};

std::vector<PcapItem> read_all(const std::vector<uint8_t>& b, PcapStatus& last, std::string& err) {
    PcapReader rd;
    std::vector<PcapItem> out;
    if (!rd.open(b.data(), b.size(), err)) { last = PcapStatus::Error; return out; }
    PcapItem it;
    while ((last = rd.next(it)) == PcapStatus::Ok) out.push_back(it);
    err = rd.error();
    return out;
}

void be32(std::vector<uint8_t>& v, uint32_t x) { for (int s = 24; s >= 0; s -= 8) v.push_back(uint8_t(x >> s)); }
void be16(std::vector<uint8_t>& v, uint16_t x) { v.push_back(uint8_t(x >> 8)); v.push_back(uint8_t(x)); }

} // namespace

TEST_CASE("pcapng: written sections read back: interfaces, ns timestamps, UDP payloads, custom blocks") {
    MemFile m;
    PcapngWriter w(m.f);
    REQUIRE(w.section("subplane test", "linux"));
    const uint32_t a = w.interface(kLinkEthernet, 65535, "eq-p0");
    const uint32_t b = w.interface(kLinkEthernet, 65535, "crypto");
    CHECK(a == 0);
    CHECK(b == 1);

    std::vector<uint8_t> ctx;
    tlv_put_u32(ctx, SprTag::FORMAT_VERSION, kSprFormatVersion);
    tlv_put(ctx, SprTag::PARTITION, "eq-p0");
    tlv_put(ctx, SprTag::PARTITION_YAML, "name: eq-p0\ntransport: {type: udp}\n"); // odd length: padded
    REQUIRE(w.custom(static_cast<uint32_t>(SprBlock::CONTEXT), ctx.data(), uint32_t(ctx.size())));

    const uint64_t ts0 = 1'762'400'000'123'456'789ull; // > 2^32 ns: both timestamp words used
    const uint8_t p1[5] = {1, 2, 3, 4, 5};
    const uint8_t p2[3] = {9, 8, 7};
    uint8_t h[kUdpWrapBytes];
    REQUIRE(udp_wrap(h, 0xEF010203u /*239.1.2.3*/, 31000, 1, sizeof p1) == kUdpWrapBytes);
    REQUIRE(w.packet(a, ts0, h, kUdpWrapBytes, p1, sizeof p1));
    REQUIRE(udp_wrap(h, 0x7F000001u, 1001, 0, sizeof p2) == kUdpWrapBytes);
    REQUIRE(w.packet(b, ts0 + 1, h, kUdpWrapBytes, p2, sizeof p2));
    const SprLoss loss{ts0 + 2, 7, 4096, a, 0};
    REQUIRE(w.custom(static_cast<uint32_t>(SprBlock::LOSS), reinterpret_cast<const uint8_t*>(&loss), sizeof loss));
    const uint64_t written = w.bytes();
    const auto file = m.close();
    CHECK(file.size() == written);
    CHECK(file.size() % 4 == 0);

    PcapStatus last;
    std::string err;
    const auto items = read_all(file, last, err);
    CHECK(last == PcapStatus::End);
    CHECK(err.empty());
    REQUIRE(items.size() == 4);

    CHECK(items[0].kind == PcapItem::CUSTOM);
    CHECK(items[0].pen == kSprPen);
    CHECK(items[0].copyable);
    CHECK(items[0].custom_kind == static_cast<uint32_t>(SprBlock::CONTEXT));
    std::string part, yaml;
    uint32_t ver = 0;
    CHECK(tlv_each(items[0].data, items[0].caplen, [&](SprTag t, const uint8_t* v, uint32_t n) {
        if (t == SprTag::PARTITION) part.assign(reinterpret_cast<const char*>(v), n);
        if (t == SprTag::PARTITION_YAML) yaml.assign(reinterpret_cast<const char*>(v), n);
        if (t == SprTag::FORMAT_VERSION && n == 4) std::memcpy(&ver, v, 4);
    }));
    CHECK(ver == kSprFormatVersion);
    CHECK(part == "eq-p0");
    CHECK(yaml == "name: eq-p0\ntransport: {type: udp}\n");

    CHECK(items[1].kind == PcapItem::PACKET);
    CHECK(items[1].iface == 0);
    CHECK(items[1].linktype == kLinkEthernet);
    CHECK(items[1].ts_ns == ts0);
    CHECK(items[1].caplen == kUdpWrapBytes + 5);
    CHECK(items[1].origlen == kUdpWrapBytes + 5);
    uint32_t off = 0, len = 0;
    REQUIRE(udp_payload(items[1].linktype, items[1].data, items[1].caplen, off, len) == UdpExtract::OK);
    CHECK(len == 5);
    CHECK(std::memcmp(items[1].data + off, p1, 5) == 0);
    // dst port and the multicast MAC
    CHECK(detail::rd16be(items[1].data + 14 + 20 + 2) == 31000);
    CHECK(items[1].data[0] == 0x01);
    CHECK(items[1].data[3] == 0x01);
    CHECK(items[1].data[5] == 0x03);

    CHECK(items[2].iface == 1);
    CHECK(items[2].ts_ns == ts0 + 1);
    REQUIRE(udp_payload(items[2].linktype, items[2].data, items[2].caplen, off, len) == UdpExtract::OK);
    CHECK(len == 3);
    CHECK(std::memcmp(items[2].data + off, p2, 3) == 0);

    CHECK(items[3].custom_kind == static_cast<uint32_t>(SprBlock::LOSS));
    REQUIRE(items[3].caplen == sizeof(SprLoss));
    SprLoss got{};
    std::memcpy(&got, items[3].data, sizeof got);
    CHECK(got.units == 7);
    CHECK(got.bytes == 4096);
}

TEST_CASE("pcapng: udp_wrap writes a valid IPv4 header and refuses what one datagram cannot hold") {
    uint8_t h[kUdpWrapBytes];
    REQUIRE(udp_wrap(h, 0x0A000002u, 24000, 40000, 100) == kUdpWrapBytes);
    uint32_t sum = 0;
    for (int i = 0; i < 20; i += 2) sum += detail::rd16be(h + 14 + i);
    while (sum >> 16) sum = (sum & 0xFFFF) + (sum >> 16);
    CHECK(sum == 0xFFFF);
    CHECK(detail::rd16be(h + 14 + 2) == 20 + 8 + 100);
    CHECK(h[0] == 0x02); // unicast: locally administered MAC
    CHECK(udp_wrap(h, 1, 1, 1, kUdpMaxPayload) == kUdpWrapBytes);
    CHECK(udp_wrap(h, 1, 1, 1, kUdpMaxPayload + 1) == 0);
}

TEST_CASE("pcapng: the FlatBuffers fixture, classic and pcapng, carries the same UDP payloads and times") {
    const auto read_file = [](const std::string& path) {
        MappedFile f;
        std::string err;
        REQUIRE_MESSAGE(f.open(path, err), err);
        return std::vector<uint8_t>(f.data(), f.data() + f.size());
    };
    const auto classic = read_file(subplane::test::fb_fixture_path("fb_sample.pcap"));
    PcapStatus last;
    std::string err;
    const auto ci = read_all(classic, last, err);
    REQUIRE(last == PcapStatus::End);
    REQUIRE(ci.size() == 120);

    // Re-encode the classic file as pcapng: the frames are carried as they are.
    MemFile m;
    PcapngWriter w(m.f);
    w.section("subplane test");
    w.interface(kLinkEthernet, 65535, "fb0");
    for (const auto& it : ci) w.packet(0, it.ts_ns, nullptr, 0, it.data, it.caplen);
    const auto ng = m.close();
    if (const char* out = std::getenv("SP_WRITE_PCAPNG_FIXTURE")) { // regenerates the checked-in copy
        std::FILE* f = std::fopen(out, "wb");
        REQUIRE(f != nullptr);
        std::fwrite(ng.data(), 1, ng.size(), f);
        std::fclose(f);
    }

    for (const std::vector<uint8_t>* bytes : {&ng, static_cast<const std::vector<uint8_t>*>(nullptr)}) {
        const auto file = bytes ? *bytes : read_file(subplane::test::fb_fixture_path("fb_sample.pcapng"));
        const auto ni = read_all(file, last, err);
        REQUIRE(last == PcapStatus::End);
        REQUIRE(ni.size() == ci.size());
        for (size_t i = 0; i < ci.size(); ++i) {
            uint32_t co, cl, no, nl;
            REQUIRE(udp_payload(ci[i].linktype, ci[i].data, ci[i].caplen, co, cl) == UdpExtract::OK);
            REQUIRE(udp_payload(ni[i].linktype, ni[i].data, ni[i].caplen, no, nl) == UdpExtract::OK);
            CHECK(ni[i].ts_ns == ci[i].ts_ns);
            REQUIRE(nl == cl);
            CHECK(std::memcmp(ni[i].data + no, ci[i].data + co, cl) == 0);
        }
    }
}

TEST_CASE("pcapng: a big-endian section, default microsecond and power-of-two resolutions") {
    std::vector<uint8_t> f;
    // SHB, big-endian, no options.
    be32(f, kBlockShb); be32(f, 28); be32(f, kByteOrderMagic); be16(f, 1); be16(f, 0);
    be32(f, 0xFFFFFFFF); be32(f, 0xFFFFFFFF); be32(f, 28);
    // IDB 0: Ethernet, no if_tsresol (microseconds).
    be32(f, kBlockIdb); be32(f, 20); be16(f, 1); be16(f, 0); be32(f, 65535); be32(f, 20);
    // IDB 1: if_tsresol 0x8A (2^-10 s).
    be32(f, kBlockIdb); be32(f, 32); be16(f, 1); be16(f, 0); be32(f, 65535);
    be16(f, 9); be16(f, 1); f.push_back(0x8A); f.push_back(0); f.push_back(0); f.push_back(0);
    be16(f, 0); be16(f, 0); be32(f, 32);
    // An unknown block (Name Resolution, type 4): skipped.
    be32(f, 4); be32(f, 16); be32(f, 0); be32(f, 16);
    // EPB on 0: ts 1.5 s in microseconds, 4 bytes.
    be32(f, kBlockEpb); be32(f, 36); be32(f, 0); be32(f, 0); be32(f, 1'500'000); be32(f, 4); be32(f, 4);
    be32(f, 0xDEADBEEF); be32(f, 36);
    // EPB on 1: ts 3 s + 512/1024 s.
    be32(f, kBlockEpb); be32(f, 32); be32(f, 1); be32(f, 0); be32(f, 3 * 1024 + 512); be32(f, 0); be32(f, 0); be32(f, 32);
    // A custom block under another PEN: handed over raw.
    be32(f, kBlockCustomNoCopy); be32(f, 20); be32(f, 32473); be32(f, 0x01020304); be32(f, 20);

    PcapStatus last;
    std::string err;
    const auto items = read_all(f, last, err);
    CHECK_MESSAGE(last == PcapStatus::End, err);
    REQUIRE(items.size() == 3);
    CHECK(items[0].ts_ns == 1'500'000'000ull);
    CHECK(items[0].caplen == 4);
    CHECK(items[0].data[0] == 0xDE);
    CHECK(items[1].iface == 1);
    CHECK(items[1].ts_ns == 3'500'000'000ull);
    CHECK(items[2].kind == PcapItem::CUSTOM);
    CHECK_FALSE(items[2].copyable);
    CHECK(items[2].pen == 32473);
    CHECK(items[2].caplen == 4);
}

TEST_CASE("pcapng: a file cut anywhere stops with an error and never reads past the cut") {
    MemFile m;
    PcapngWriter w(m.f);
    w.section("subplane test");
    w.interface(kLinkEthernet, 65535, "p");
    const uint8_t pl[7] = {1, 2, 3, 4, 5, 6, 7};
    uint8_t h[kUdpWrapBytes];
    udp_wrap(h, 0x7F000001u, 9, 9, sizeof pl);
    for (int i = 0; i < 3; ++i) w.packet(0, uint64_t(i), h, kUdpWrapBytes, pl, sizeof pl);
    const auto full = m.close();
    for (size_t cut = 1; cut < full.size(); ++cut) {
        std::vector<uint8_t> part(full.begin(), full.begin() + static_cast<std::ptrdiff_t>(cut));
        PcapStatus last;
        std::string err;
        const auto items = read_all(part, last, err);
        const bool at_boundary = last == PcapStatus::End;
        CHECK((at_boundary || last == PcapStatus::Error));
        for (const auto& it : items) CHECK(it.data + it.caplen <= part.data() + part.size());
        if (!at_boundary) CHECK_FALSE(err.empty());
    }
}

TEST_CASE("pcapng: not a capture file") {
    const std::vector<uint8_t> junk = {'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd', '!'};
    PcapReader rd;
    std::string err;
    CHECK_FALSE(rd.open(junk.data(), junk.size(), err));
    CHECK(err.find("bad magic") != std::string::npos);
}
