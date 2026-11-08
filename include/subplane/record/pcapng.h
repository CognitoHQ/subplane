#pragma once

// subplane recordings: a pcapng reader and writer (and the classic pcap reader).
// Header-only, C++20, libc only. The format and the custom blocks are specified in
// docs/context/recording-format.md; a recording is a valid pcapng file.
//
//   PcapReader rd;                    // classic pcap or pcapng, either byte order
//   rd.open(bytes, size, err);        // over a buffer the caller owns (mmap or a vector)
//   PcapItem it;
//   while (rd.next(it) == PcapStatus::Ok) {
//       if (it.kind == PcapItem::PACKET) { uint32_t off, len;
//           if (udp_payload(it.linktype, it.data, it.caplen, off, len) == UdpExtract::OK) use(it.data + off, len); }
//       else if (it.pen == kRecPen) on_custom(it.custom_kind, it.data, it.caplen);
//   }
//
//   PcapngWriter w(file);             // a FILE* the caller opened; never closes it
//   w.section("subplane record");
//   const uint32_t ifc = w.interface(kLinkEthernet, 65535, "partition-name");
//   uint8_t hdr[kUdpWrapBytes];
//   const uint32_t n = udp_wrap(hdr, dst_ip, dst_port, src_port, len);
//   w.packet(ifc, ts_ns, hdr, n, payload, len);
//
// Neither side allocates per packet; the reader never copies packet bytes.

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace subplane::record {

inline constexpr uint32_t kBlockShb = 0x0A0D0D0Au;
inline constexpr uint32_t kBlockIdb = 0x00000001u;
inline constexpr uint32_t kBlockSpb = 0x00000003u;
inline constexpr uint32_t kBlockEpb = 0x00000006u;
inline constexpr uint32_t kBlockCustomCopy = 0x00000BADu;   // editcap and friends keep it
inline constexpr uint32_t kBlockCustomNoCopy = 0x40000BADu;
inline constexpr uint32_t kByteOrderMagic = 0x1A2B3C4Du;

inline constexpr uint16_t kLinkEthernet = 1;

// INTENT: a pcapng custom block is identified by an IANA Private Enterprise Number.
// subplane has none yet (to be registered under CognitoHQ). 0 is IANA's reserved
// value, used here as an obvious placeholder; files written with it are pre-release.
// When the real PEN lands, readers keep accepting 0 so earlier recordings still read.
inline constexpr uint32_t kRecPen = 0;

// Custom block kinds (recording-format.md). The body follows {pen, kind, body_len}.
enum class RecBlock : uint32_t {
    CONTEXT = 1,    // TLV list: what is needed to read the segment (RecTag)
    LOSS = 2,       // RecLoss: the recorder was lapped by the tap
    TRANSPORT = 3,  // RecTransport: the partition's transport went down or came up
};

// TLV tags of a CONTEXT block. Unknown tags are skipped, so new ones are additive.
enum class RecTag : uint16_t {
    FORMAT_VERSION = 1,   // u32, kRecFormatVersion
    AGENT_NAME = 2,       // text
    AGENT_VERSION = 3,    // text
    AGENT_EPOCH = 4,      // u64 ns, the tap ring's epoch
    PARTITION = 5,        // text, partition name (also the interface's if_name)
    PARTITION_YAML = 6,   // text, the partition's YAML as the agent loaded it
    SCHEMA_XML = 7,       // bytes, the schema file the decoder loaded (sbe_routing only)
    PARTITION_SHA256 = 8, // 32 bytes, of PARTITION_YAML
    SCHEMA_SHA256 = 9,    // 32 bytes, of SCHEMA_XML
    TS_SOURCE = 10,       // text: "kernel" or "poll"
    TRANSPORT_TYPE = 11,  // text: udp, udp_multicast, tcp_stream, aeron
    INTERFACE = 12,       // u32, the pcapng interface id this context describes
};
inline constexpr uint32_t kRecFormatVersion = 1;

struct RecLoss {
    uint64_t at_ns;      // when the recorder noticed
    uint64_t units;      // units lost (from the tap's sequence)
    uint64_t bytes;      // tap ring bytes skipped
    uint32_t interface;  // pcapng interface id
    uint32_t reserved;
};
static_assert(sizeof(RecLoss) == 32);

struct RecTransport {
    uint64_t at_ns;
    uint32_t interface;
    uint32_t up;         // 1 up, 0 down
};
static_assert(sizeof(RecTransport) == 16);

// ------------------------------------------------------------------ helpers

namespace detail {
inline uint16_t rd16(const uint8_t* p, bool swap) {
    uint16_t v;
    std::memcpy(&v, p, 2);
    return swap ? __builtin_bswap16(v) : v;
}
inline uint32_t rd32(const uint8_t* p, bool swap) {
    uint32_t v;
    std::memcpy(&v, p, 4);
    return swap ? __builtin_bswap32(v) : v;
}
inline uint16_t rd16be(const uint8_t* p) { return static_cast<uint16_t>((p[0] << 8) | p[1]); }
inline void wr16be(uint8_t* p, uint16_t v) { p[0] = uint8_t(v >> 8); p[1] = uint8_t(v); }
inline constexpr uint32_t pad4(uint32_t n) { return (n + 3u) & ~3u; }
} // namespace detail

// ------------------------------------------------------------------ UDP in link frames

enum class UdpExtract { OK, NOT_UDP, IPV6, FRAGMENT, TRUNCATED };

// Finds the UDP payload of one captured frame. Link types: 1 Ethernet (with stacked
// 802.1Q/QinQ tags), 113 Linux SLL, 101 raw IP, 228 raw IPv4. IPv4 only. A payload cut by
// the snaplen is returned as captured (len is what is present).
inline UdpExtract udp_payload(uint32_t link, const uint8_t* d, uint32_t n, uint32_t& off, uint32_t& len) {
    using detail::rd16be;
    off = 0;
    len = 0;
    uint32_t l3 = 0;
    bool v4 = false, v6 = false;
    if (link == 1) {
        if (n < 14) return UdpExtract::TRUNCATED;
        uint32_t o = 12;
        uint16_t et = rd16be(d + o);
        while ((et == 0x8100 || et == 0x88A8) && o + 4 + 2 <= n) { o += 4; et = rd16be(d + o); }
        l3 = o + 2;
        v4 = et == 0x0800;
        v6 = et == 0x86DD;
    } else if (link == 113) {
        if (n < 16) return UdpExtract::TRUNCATED;
        const uint16_t et = rd16be(d + 14);
        l3 = 16;
        v4 = et == 0x0800;
        v6 = et == 0x86DD;
    } else if (link == 101 || link == 228) {
        if (n < 1) return UdpExtract::TRUNCATED;
        v4 = (d[0] >> 4) == 4;
        v6 = link == 101 && (d[0] >> 4) == 6;
    } else {
        return UdpExtract::NOT_UDP;
    }
    if (v6) return UdpExtract::IPV6;
    if (!v4 || l3 + 20 > n || (d[l3] >> 4) != 4) return UdpExtract::NOT_UDP;
    const uint32_t ihl = (d[l3] & 0x0Fu) * 4u;
    if (ihl < 20 || l3 + ihl + 8 > n || d[l3 + 9] != 17) return UdpExtract::NOT_UDP;
    if ((rd16be(d + l3 + 6) & 0x3FFF) != 0) return UdpExtract::FRAGMENT; // MF set or offset != 0
    const uint32_t u = l3 + ihl;
    const uint32_t ulen = rd16be(d + u + 4);
    if (ulen < 8) return UdpExtract::TRUNCATED;
    off = u + 8;
    len = ulen - 8;
    if (off + len > n) len = n - off;
    return UdpExtract::OK;
}

// Synthetic Ethernet/IPv4/UDP header for a unit that arrived without one (the agent sees
// UDP payloads, Aeron messages and TCP stream records). Source 0.0.0.0: the agent never
// learns the sender. A multicast destination gets its 01:00:5e MAC, others a locally
// administered one. The IPv4 checksum is set; the UDP checksum is 0 (allowed on IPv4).
inline constexpr uint32_t kUdpWrapBytes = 42;
inline constexpr uint32_t kUdpMaxPayload = 65535u - 20u - 8u;

// Returns kUdpWrapBytes, or 0 when payload_len cannot fit one IPv4 datagram.
inline uint32_t udp_wrap(uint8_t* out, uint32_t dst_ip, uint16_t dst_port, uint16_t src_port,
                         uint32_t payload_len) noexcept {
    using detail::wr16be;
    if (payload_len > kUdpMaxPayload) return 0;
    uint8_t* e = out;
    if ((dst_ip >> 28) == 0xE) {
        e[0] = 0x01; e[1] = 0x00; e[2] = 0x5e;
        e[3] = uint8_t((dst_ip >> 16) & 0x7F); e[4] = uint8_t(dst_ip >> 8); e[5] = uint8_t(dst_ip);
    } else {
        const uint8_t m[6] = {0x02, 0, 0, 0, 0, 0x02};
        std::memcpy(e, m, 6);
    }
    const uint8_t src_mac[6] = {0x02, 0, 0, 0, 0, 0x01};
    std::memcpy(e + 6, src_mac, 6);
    wr16be(e + 12, 0x0800);
    uint8_t* ip = out + 14;
    ip[0] = 0x45; ip[1] = 0;
    wr16be(ip + 2, uint16_t(20 + 8 + payload_len));
    wr16be(ip + 4, 0);          // id
    wr16be(ip + 6, 0x4000);     // DF
    ip[8] = 64; ip[9] = 17;     // ttl, udp
    wr16be(ip + 10, 0);
    std::memset(ip + 12, 0, 4); // src 0.0.0.0
    ip[16] = uint8_t(dst_ip >> 24); ip[17] = uint8_t(dst_ip >> 16); ip[18] = uint8_t(dst_ip >> 8); ip[19] = uint8_t(dst_ip);
    uint32_t sum = 0;
    for (int i = 0; i < 20; i += 2) sum += detail::rd16be(ip + i);
    while (sum >> 16) sum = (sum & 0xFFFF) + (sum >> 16);
    wr16be(ip + 10, uint16_t(~sum));
    uint8_t* udp = ip + 20;
    wr16be(udp, src_port);
    wr16be(udp + 2, dst_port);
    wr16be(udp + 4, uint16_t(8 + payload_len));
    wr16be(udp + 6, 0);
    return kUdpWrapBytes;
}

// ------------------------------------------------------------------ reader

enum class PcapStatus { Ok, End, Error };

struct PcapItem {
    enum Kind : uint8_t { PACKET, CUSTOM } kind = PACKET;
    bool copyable = true;      // CUSTOM: block type 0x00000BAD (true) or 0x40000BAD
    uint16_t linktype = 0;     // PACKET
    uint32_t iface = 0;        // PACKET: interface id within the section (0 for classic pcap)
    uint64_t ts_ns = 0;        // PACKET
    const uint8_t* data = nullptr; // PACKET: the frame; CUSTOM: the body after {pen, kind, body_len}
    uint32_t caplen = 0;       // bytes at data
    uint32_t origlen = 0;      // PACKET: length on the wire
    uint32_t pen = 0;          // CUSTOM
    uint32_t custom_kind = 0;  // CUSTOM (RecBlock for kRecPen)
};

class PcapReader {
public:
    static constexpr uint32_t kMaxInterfaces = 64;

    // Over [p, p+n); the buffer must outlive the reader and the items it returns.
    bool open(const uint8_t* p, size_t n, std::string& err) {
        p_ = p; n_ = n; pos_ = 0; n_if_ = 0; err_.clear();
        if (n < 4) { err = "file too short for a pcap header"; return false; }
        uint32_t magic;
        std::memcpy(&magic, p, 4);
        if (magic == kBlockShb) { ng_ = true; return true; }
        ng_ = false;
        switch (magic) {
            case 0xa1b2c3d4: swap_ = false; nano_ = false; break;
            case 0xd4c3b2a1: swap_ = true;  nano_ = false; break;
            case 0xa1b23c4d: swap_ = false; nano_ = true;  break;
            case 0x4d3cb2a1: swap_ = true;  nano_ = true;  break;
            default: err = "not a pcap or pcapng file (bad magic)"; return false;
        }
        if (n < 24) { err = "file too short for a pcap header"; return false; }
        classic_link_ = detail::rd32(p + 20, swap_) & 0xFFFFu;
        pos_ = 24;
        return true;
    }

    bool is_pcapng() const { return ng_; }
    const std::string& error() const { return err_; }

    PcapStatus next(PcapItem& it) {
        return ng_ ? next_ng(it) : next_classic(it);
    }

private:
    PcapStatus fail(const char* what) { err_ = what; pos_ = n_; return PcapStatus::Error; }

    PcapStatus next_classic(PcapItem& it) {
        if (pos_ == n_) return PcapStatus::End;
        if (n_ - pos_ < 16) return fail("truncated pcap record header");
        const uint8_t* r = p_ + pos_;
        const uint32_t sec = detail::rd32(r, swap_), frac = detail::rd32(r + 4, swap_);
        const uint32_t incl = detail::rd32(r + 8, swap_), orig = detail::rd32(r + 12, swap_);
        if (incl > n_ - pos_ - 16) return fail("truncated pcap record");
        it = PcapItem{};
        it.kind = PcapItem::PACKET;
        it.linktype = uint16_t(classic_link_);
        it.ts_ns = uint64_t{sec} * 1'000'000'000ull + (nano_ ? frac : uint64_t{frac} * 1000ull);
        it.data = r + 16;
        it.caplen = incl;
        it.origlen = orig;
        pos_ += 16 + incl;
        return PcapStatus::Ok;
    }

    PcapStatus next_ng(PcapItem& it) {
        for (;;) {
            if (pos_ == n_) return PcapStatus::End;
            if (n_ - pos_ < 12) return fail("truncated pcapng block header");
            const uint8_t* b = p_ + pos_;
            uint32_t type;
            std::memcpy(&type, b, 4);
            if (type == kBlockShb) {
                // The byte-order magic decides how the rest of the section is read.
                const uint32_t bom = detail::rd32(b + 8, false);
                if (bom == kByteOrderMagic) swap_ = false;
                else if (bom == __builtin_bswap32(kByteOrderMagic)) swap_ = true;
                else return fail("pcapng section header: bad byte-order magic");
                n_if_ = 0;
            } else {
                type = detail::rd32(b, swap_);
            }
            const uint32_t len = detail::rd32(b + 4, swap_);
            if (len < 12 || (len & 3u) != 0) return fail("pcapng block: bad length");
            if (len > n_ - pos_) return fail("truncated pcapng block");
            if (detail::rd32(b + len - 4, swap_) != len) return fail("pcapng block: trailing length mismatch");
            const uint8_t* body = b + 8;
            const uint32_t blen = len - 12;
            pos_ += len;
            switch (type) {
                case kBlockShb:
                    if (blen < 16) return fail("pcapng section header too short");
                    if (detail::rd16(body + 4, swap_) != 1) return fail("pcapng: unsupported major version");
                    continue;
                case kBlockIdb: {
                    if (blen < 8) return fail("pcapng interface block too short");
                    if (n_if_ == kMaxInterfaces) return fail("pcapng: too many interfaces");
                    Iface f{};
                    f.linktype = detail::rd16(body, swap_);
                    f.tsresol = 6;
                    if (!read_tsresol(body + 8, blen - 8, f.tsresol)) return fail("pcapng interface block: bad options");
                    if ((f.tsresol & 0x80) ? (f.tsresol & 0x7F) > 32 : f.tsresol > 18)
                        return fail("pcapng interface block: unsupported if_tsresol");
                    if_[n_if_++] = f;
                    continue;
                }
                case kBlockEpb: {
                    if (blen < 20) return fail("pcapng packet block too short");
                    const uint32_t ifc = detail::rd32(body, swap_);
                    if (ifc >= n_if_) return fail("pcapng packet block: unknown interface");
                    const uint64_t ts = (uint64_t{detail::rd32(body + 4, swap_)} << 32) | detail::rd32(body + 8, swap_);
                    const uint32_t cap = detail::rd32(body + 12, swap_);
                    if (cap > blen - 20) return fail("pcapng packet block: captured length past the block");
                    it = PcapItem{};
                    it.kind = PcapItem::PACKET;
                    it.iface = ifc;
                    it.linktype = if_[ifc].linktype;
                    it.ts_ns = to_ns(ts, if_[ifc].tsresol);
                    it.data = body + 20;
                    it.caplen = cap;
                    it.origlen = detail::rd32(body + 16, swap_);
                    return PcapStatus::Ok;
                }
                case kBlockSpb: {
                    if (blen < 4) return fail("pcapng simple packet block too short");
                    if (n_if_ == 0) return fail("pcapng simple packet block before any interface");
                    const uint32_t orig = detail::rd32(body, swap_);
                    it = PcapItem{};
                    it.kind = PcapItem::PACKET;
                    it.linktype = if_[0].linktype;
                    it.data = body + 4;
                    it.caplen = orig < blen - 4 ? orig : blen - 4;
                    it.origlen = orig;
                    return PcapStatus::Ok; // SPB carries no timestamp: ts_ns 0
                }
                case kBlockCustomCopy:
                case kBlockCustomNoCopy: {
                    if (blen < 4) return fail("pcapng custom block too short");
                    it = PcapItem{};
                    it.kind = PcapItem::CUSTOM;
                    it.copyable = type == kBlockCustomCopy;
                    it.pen = detail::rd32(body, swap_);
                    if (it.pen != kRecPen) { // someone else's block: hand over the raw data
                        it.data = body + 4;
                        it.caplen = blen - 4;
                        return PcapStatus::Ok;
                    }
                    if (blen < 12) return fail("subplane custom block too short");
                    it.custom_kind = detail::rd32(body + 4, swap_);
                    const uint32_t bl = detail::rd32(body + 8, swap_);
                    if (bl > blen - 12) return fail("subplane custom block: body past the block");
                    it.data = body + 12;
                    it.caplen = bl;
                    return PcapStatus::Ok;
                }
                default:
                    continue; // name resolution, statistics, decryption secrets...: skipped by length
            }
        }
    }

    bool read_tsresol(const uint8_t* o, uint32_t n, uint8_t& tsresol) const {
        uint32_t i = 0;
        while (i + 4 <= n) {
            const uint16_t code = detail::rd16(o + i, swap_), len = detail::rd16(o + i + 2, swap_);
            if (code == 0) return true;
            if (detail::pad4(len) > n - i - 4) return false;
            if (code == 9 && len >= 1) tsresol = o[i + 4];
            i += 4 + detail::pad4(len);
        }
        return true;
    }

    static uint64_t to_ns(uint64_t t, uint8_t res) {
        if (res & 0x80) { // 2^-v
            const unsigned v = res & 0x7F;
            const uint64_t whole = t >> v, frac = t & ((uint64_t{1} << v) - 1);
            return whole * 1'000'000'000ull + ((frac * 1'000'000'000ull) >> v);
        }
        uint64_t s = 1;
        if (res <= 9) { for (unsigned i = res; i < 9; ++i) s *= 10; return t * s; }
        for (unsigned i = 9; i < res; ++i) s *= 10;
        return t / s;
    }

    struct Iface { uint16_t linktype; uint8_t tsresol; };

    const uint8_t* p_ = nullptr;
    size_t n_ = 0, pos_ = 0;
    bool ng_ = false, swap_ = false, nano_ = false;
    uint32_t classic_link_ = 0;
    Iface if_[kMaxInterfaces]{};
    uint32_t n_if_ = 0;
    std::string err_;
};

// A read-only mapping of a whole file, for PcapReader.
class MappedFile {
public:
    MappedFile() = default;
    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;
    ~MappedFile() { close(); }

    bool open(const std::string& path, std::string& err) {
        close();
        const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) { err = "cannot open " + path + ": " + std::strerror(errno); return false; }
        struct stat st{};
        if (::fstat(fd, &st) != 0) { err = "cannot stat " + path; ::close(fd); return false; }
        size_ = static_cast<size_t>(st.st_size);
        if (size_ > 0) {
            void* m = ::mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd, 0);
            if (m == MAP_FAILED) { err = "cannot map " + path + ": " + std::strerror(errno); ::close(fd); size_ = 0; return false; }
            data_ = static_cast<const uint8_t*>(m);
        }
        ::close(fd);
        return true;
    }
    void close() {
        if (data_ != nullptr) ::munmap(const_cast<uint8_t*>(data_), size_);
        data_ = nullptr;
        size_ = 0;
    }
    const uint8_t* data() const { return data_; }
    size_t size() const { return size_; }

private:
    const uint8_t* data_ = nullptr;
    size_t size_ = 0;
};

// ------------------------------------------------------------------ writer

// Writes one pcapng section, little-endian, to a FILE* the caller owns. Every call returns
// false once a write has failed (the file is then incomplete; the caller discards it).
class PcapngWriter {
public:
    explicit PcapngWriter(std::FILE* f) : f_(f) {}

    // The Section Header Block; starts a section (interface ids restart at 0).
    bool section(std::string_view userappl, std::string_view os = {}) {
        n_if_ = 0;
        const uint32_t opts = opt_size(userappl) + opt_size(os) + 4;
        const uint32_t len = 12 + 16 + opts;
        uint8_t h[24];
        put32(h, kBlockShb); put32(h + 4, len); put32(h + 8, kByteOrderMagic);
        put16(h + 12, 1); put16(h + 14, 0);
        const int64_t unknown = -1;
        std::memcpy(h + 16, &unknown, 8);
        return write(h, 24) && opt(4, userappl) && opt(3, os) && end_opts() && put_len(len);
    }

    // An Interface Description Block with nanosecond timestamps. Returns its id.
    uint32_t interface(uint16_t linktype, uint32_t snaplen, std::string_view name) {
        const uint8_t tsresol = 9;
        const uint32_t opts = opt_size(name) + opt_size(std::string_view(reinterpret_cast<const char*>(&tsresol), 1)) + 4;
        const uint32_t len = 12 + 8 + opts;
        uint8_t h[16];
        put32(h, kBlockIdb); put32(h + 4, len);
        put16(h + 8, linktype); put16(h + 10, 0); put32(h + 12, snaplen);
        ok_ = write(h, 16) && opt(2, name) && opt(9, std::string_view(reinterpret_cast<const char*>(&tsresol), 1)) &&
              end_opts() && put_len(len);
        return n_if_++;
    }

    // An Enhanced Packet Block: the frame is head (may be empty) then body, without copying.
    bool packet(uint32_t iface, uint64_t ts_ns, const uint8_t* head, uint32_t head_len,
                const uint8_t* body, uint32_t body_len) {
        const uint32_t cap = head_len + body_len;
        const uint32_t len = 12 + 20 + detail::pad4(cap);
        uint8_t h[28];
        put32(h, kBlockEpb); put32(h + 4, len); put32(h + 8, iface);
        put32(h + 12, uint32_t(ts_ns >> 32)); put32(h + 16, uint32_t(ts_ns));
        put32(h + 20, cap); put32(h + 24, cap);
        return write(h, 28) && write(head, head_len) && write(body, body_len) && pad(cap) && put_len(len);
    }

    // A custom block: {pen, kind, body_len, body, padding}.
    bool custom(uint32_t kind, const uint8_t* body, uint32_t body_len, bool copyable = true, uint32_t pen = kRecPen) {
        const uint32_t len = 12 + 12 + detail::pad4(body_len);
        uint8_t h[20];
        put32(h, copyable ? kBlockCustomCopy : kBlockCustomNoCopy); put32(h + 4, len);
        put32(h + 8, pen); put32(h + 12, kind); put32(h + 16, body_len);
        return write(h, 20) && write(body, body_len) && pad(body_len) && put_len(len);
    }

    bool ok() const { return ok_; }
    uint64_t bytes() const { return bytes_; }

private:
    static void put16(uint8_t* p, uint16_t v) { std::memcpy(p, &v, 2); }
    static void put32(uint8_t* p, uint32_t v) { std::memcpy(p, &v, 4); }
    static uint32_t opt_size(std::string_view v) { return v.empty() ? 0 : 4 + detail::pad4(uint32_t(v.size())); }

    bool write(const void* p, size_t n) {
        if (!ok_) return false;
        if (n != 0 && std::fwrite(p, 1, n, f_) != n) ok_ = false;
        bytes_ += n;
        return ok_;
    }
    bool pad(uint32_t n) {
        static constexpr uint8_t z[4] = {};
        return write(z, detail::pad4(n) - n);
    }
    bool opt(uint16_t code, std::string_view v) {
        if (v.empty()) return ok_;
        uint8_t h[4];
        put16(h, code); put16(h + 2, uint16_t(v.size()));
        return write(h, 4) && write(v.data(), v.size()) && pad(uint32_t(v.size()));
    }
    bool end_opts() { static constexpr uint8_t z[4] = {}; return write(z, 4); }
    bool put_len(uint32_t len) { uint8_t h[4]; put32(h, len); return write(h, 4); }

    std::FILE* f_;
    bool ok_ = true;
    uint32_t n_if_ = 0;
    uint64_t bytes_ = 0;
};

// ------------------------------------------------------------------ CONTEXT TLVs

// {u16 tag, u16 reserved, u32 len, value, padding to 4}. Built off the data path.
inline void tlv_put(std::vector<uint8_t>& out, RecTag tag, const void* v, uint32_t len) {
    const size_t at = out.size();
    out.resize(at + 8 + detail::pad4(len), 0);
    const uint16_t t = static_cast<uint16_t>(tag);
    std::memcpy(out.data() + at, &t, 2);
    std::memcpy(out.data() + at + 4, &len, 4);
    if (len != 0) std::memcpy(out.data() + at + 8, v, len);
}
inline void tlv_put(std::vector<uint8_t>& out, RecTag tag, std::string_view s) {
    tlv_put(out, tag, s.data(), uint32_t(s.size()));
}
inline void tlv_put_u64(std::vector<uint8_t>& out, RecTag tag, uint64_t v) { tlv_put(out, tag, &v, 8); }
inline void tlv_put_u32(std::vector<uint8_t>& out, RecTag tag, uint32_t v) { tlv_put(out, tag, &v, 4); }

// Walks a CONTEXT body. Returns false on a malformed list.
template <class Fn>
bool tlv_each(const uint8_t* p, uint32_t n, Fn&& fn) {
    uint32_t i = 0;
    while (i < n) {
        if (n - i < 8) return false;
        uint16_t t;
        uint32_t len;
        std::memcpy(&t, p + i, 2);
        std::memcpy(&len, p + i + 4, 4);
        if (len > n - i - 8) return false;
        fn(static_cast<RecTag>(t), p + i + 8, len);
        const uint32_t step = 8 + detail::pad4(len);
        if (step > n - i) return true; // last value without its padding
        i += step;
    }
    return true;
}

} // namespace subplane::record
