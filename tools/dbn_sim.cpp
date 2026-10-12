// ft_dbn_sim: serves DBN (Databento Binary Encoding) over TCP the way the
// Databento live gateway does after Start(): a "DBN" prelude with metadata,
// then a stream of records. Source is either an uncompressed .dbn file or
// synthetic v3 records (MBO, trade, MBP-1, periodic SYSTEM heartbeats).
//
//   ft_dbn_sim --port 9550 --file CLX5_mbo.dbn [--loop N] [--rate R] [--timing file]
//   ft_dbn_sim --port 9550 --synthetic --instruments 10 --count 100000 --mix mbo=60,trade=30,mbp1=10
//              [--gap-every K] [--heartbeat-ms N]
//   common: [--bind 127.0.0.1] [--prelude dbn|none] [--clients N]   (0 = serve forever)
//
// Clients are served one after another. Exit 0 after --clients connections,
// 1 on a runtime error, 2 on bad arguments.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "tool_common.h"

using namespace fttool;

namespace {

volatile std::sig_atomic_t g_stop = 0;
void on_sigint(int) { g_stop = 1; }

void usage() {
    std::fputs(
        "usage: ft_dbn_sim --port N (--file <x.dbn> | --synthetic) [options]\n"
        "  --bind <ip>           listen address (default 127.0.0.1)\n"
        "  --port <n>            listen port\n"
        "  --clients <n>         connections to serve then exit, 0 = forever (default 0)\n"
        "  --prelude dbn|none    send the DBN prelude + metadata per connection (default dbn)\n"
        "  --rate <rec/s>        0 = as fast as possible (default 0)\n"
        "file source:\n"
        "  --file <path>         uncompressed DBN file (zstd .dbn.zst must be decompressed first)\n"
        "  --loop <n>            replay the file n times, 0 = forever (default 1)\n"
        "  --timing file         reproduce ts_event gaps (each capped at 1 s)\n"
        "synthetic source (DBN v3 records):\n"
        "  --instruments <n>     instrument ids 1000..1000+n-1 (default 10)\n"
        "  --count <n>           records to send per connection, 0 = forever (default 0)\n"
        "  --mix mbo=60,trade=30,mbp1=10\n"
        "  --gap-every <n>       skip one sequence number every n records\n"
        "  --heartbeat-ms <n>    SYSTEM heartbeat record every n ms, 0 = off\n",
        stderr);
}

struct Opts {
    std::string bind = "127.0.0.1", file, prelude = "dbn", timing = "none";
    uint64_t port = 0, clients = 0, rate = 0, loop = 1, instruments = 10, count = 0, gap_every = 0, heartbeat_ms = 0;
    unsigned mix_mbo = 60, mix_trade = 30, mix_mbp1 = 10;
    bool synthetic = false;
};

bool parse_mix(const std::string& s, Opts& o, std::string& err) {
    o.mix_mbo = o.mix_trade = o.mix_mbp1 = 0;
    size_t i = 0;
    while (i < s.size()) {
        size_t comma = s.find(',', i);
        if (comma == std::string::npos) comma = s.size();
        const std::string item = s.substr(i, comma - i);
        const size_t eq = item.find('=');
        if (eq == std::string::npos) { err = "bad --mix item: " + item; return false; }
        const std::string name = item.substr(0, eq);
        const unsigned v = static_cast<unsigned>(std::strtoul(item.c_str() + eq + 1, nullptr, 10));
        if (name == "mbo") o.mix_mbo = v; else if (name == "trade") o.mix_trade = v; else if (name == "mbp1") o.mix_mbp1 = v;
        else { err = "unknown --mix key: " + name; return false; }
        i = comma + 1;
    }
    if (o.mix_mbo + o.mix_trade + o.mix_mbp1 == 0) { err = "--mix sums to zero"; return false; }
    return true;
}

// ---- byte helpers -----------------------------------------------------------
struct Buf {
    std::vector<uint8_t> b;
    void u8(uint8_t v) { b.push_back(v); }
    void u(uint64_t v, unsigned n) { for (unsigned i = 0; i < n; ++i) b.push_back(static_cast<uint8_t>(v >> (8 * i))); }
    void zeros(size_t n) { b.insert(b.end(), n, 0); }
    void str(const char* s, size_t field) { size_t n = std::strlen(s); if (n > field) n = field; b.insert(b.end(), s, s + n); zeros(field - n); }
};

uint64_t wall_ns() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

// DBN record header: u8 length/4, u8 rtype, u16 publisher, u32 instrument, u64 ts_event.
void header(Buf& x, uint32_t total, uint8_t rtype, uint32_t iid, uint64_t ts) {
    x.u8(static_cast<uint8_t>(total / 4)); x.u8(rtype); x.u(1, 2); x.u(iid, 4); x.u(ts, 8);
}

// Synthetic prelude: "DBN" v3, metadata with dataset SYNTH.DBN, schema MBO.
std::vector<uint8_t> synthetic_prelude() {
    Buf m;
    m.str("SYNTH.DBN", 16);       // dataset
    m.u(0, 2);                    // schema: MBO
    m.u(wall_ns(), 8);            // start
    m.u(0xFFFFFFFFFFFFFFFFull, 8);// end: null
    m.u(0, 8);                    // limit
    m.u8(1); m.u8(1); m.u8(0);    // stype_in, stype_out, ts_out
    m.u(71, 2);                   // symbol_cstr_len
    m.zeros(53);                  // reserved
    m.u(0, 4); m.u(0, 4); m.u(0, 4); m.u(0, 4); m.u(0, 4); // schema_def, symbols, partial, not_found, mappings
    Buf p;
    p.b = {'D', 'B', 'N', 3};
    p.u(m.b.size(), 4);
    p.b.insert(p.b.end(), m.b.begin(), m.b.end());
    return p.b;
}

struct Lcg {
    uint64_t s = 0x9E3779B97F4A7C15ull;
    uint64_t next() { s = s * 6364136223846793005ull + 1442695040888963407ull; return s >> 11; }
};

class Synth {
public:
    explicit Synth(const Opts& o) : o_(o) {}
    // Builds the k-th record (1-based). Sequence is global and monotonic; --gap-every skips one.
    const std::vector<uint8_t>& build(uint64_t k) {
        x_.b.clear();
        const uint32_t iid = 1000u + static_cast<uint32_t>(k % o_.instruments);
        const uint64_t ts = wall_ns();
        if (o_.gap_every && k % o_.gap_every == 0) ++seq_;
        const uint32_t seq = static_cast<uint32_t>(++seq_);
        const unsigned total = o_.mix_mbo + o_.mix_trade + o_.mix_mbp1;
        const unsigned pick = static_cast<unsigned>(rng_.next() % total);
        const int64_t px = 1000000000ll + static_cast<int64_t>(rng_.next() % 100000) * 1000; // 1e-9 units
        const uint32_t sz = 1u + static_cast<uint32_t>(rng_.next() % 50);
        if (pick < o_.mix_mbo) {
            header(x_, 56, 0xA0, iid, ts);
            x_.u(rng_.next(), 8);            // order_id
            x_.u(static_cast<uint64_t>(px), 8);
            x_.u(sz, 4);
            x_.u8(0); x_.u8(0);              // flags, channel_id
            x_.u8('A'); x_.u8(rng_.next() % 2 ? 'B' : 'A');
            x_.u(ts, 8);                     // ts_recv
            x_.u(0, 4);                      // ts_in_delta
            x_.u(seq, 4);
        } else if (pick < o_.mix_mbo + o_.mix_trade) {
            header(x_, 48, 0x00, iid, ts);
            x_.u(static_cast<uint64_t>(px), 8);
            x_.u(sz, 4);
            x_.u8('T'); x_.u8(rng_.next() % 2 ? 'B' : 'A'); x_.u8(0); x_.u8(0);
            x_.u(ts, 8);
            x_.u(0, 4);
            x_.u(seq, 4);
        } else {
            header(x_, 80, 0x01, iid, ts);
            x_.u(static_cast<uint64_t>(px), 8);
            x_.u(sz, 4);
            x_.u8('A'); x_.u8('B'); x_.u8(0); x_.u8(0);
            x_.u(ts, 8);
            x_.u(0, 4);
            x_.u(seq, 4);
            x_.u(static_cast<uint64_t>(px - 1000), 8); x_.u(static_cast<uint64_t>(px + 1000), 8); // bid/ask px
            x_.u(sz, 4); x_.u(sz, 4); x_.u(1, 4); x_.u(1, 4);                                   // sizes, counts
        }
        return x_.b;
    }
    const std::vector<uint8_t>& heartbeat() {
        x_.b.clear();
        header(x_, 320, 0x17, 0, wall_ns());
        x_.str("Heartbeat", 303);
        x_.u8(0);                            // code
        return x_.b;
    }
private:
    const Opts& o_;
    Buf x_;
    Lcg rng_;
    uint64_t seq_ = 0;
};

// Uncompressed DBN file split into prelude and records.
struct DbnFile {
    std::vector<uint8_t> bytes;
    size_t records_off = 0;
    std::vector<std::pair<size_t, uint32_t>> recs; // offset, len
    bool load(const std::string& path, std::string& err) {
        std::ifstream f(path, std::ios::binary);
        if (!f) { err = "cannot open " + path; return false; }
        bytes.assign((std::istreambuf_iterator<char>(f)), {});
        if (bytes.size() < 8) { err = "file too small for a DBN prelude"; return false; }
        uint32_t magic; std::memcpy(&magic, bytes.data(), 4);
        if (magic == 0xFD2FB528u) { err = "zstd-compressed DBN; decompress first (dbn CLI: dbn -o out.dbn in.dbn.zst)"; return false; }
        if (std::memcmp(bytes.data(), "DBN", 3) != 0) { err = "not a DBN file (missing prelude)"; return false; }
        uint32_t mlen; std::memcpy(&mlen, bytes.data() + 4, 4);
        records_off = 8 + mlen;
        if (records_off > bytes.size()) { err = "metadata length runs past end of file"; return false; }
        size_t off = records_off;
        while (off + 16 <= bytes.size()) {
            const uint32_t len = static_cast<uint32_t>(bytes[off]) * 4u;
            if (len < 16 || off + len > bytes.size()) { err = "bad record length at offset " + std::to_string(off); return false; }
            recs.emplace_back(off, len);
            off += len;
        }
        return true;
    }
    uint64_t ts_event(size_t i) const { uint64_t t; std::memcpy(&t, bytes.data() + recs[i].first + 8, 8); return t; }
};

bool send_all(int fd, const uint8_t* p, size_t n) {
    while (n > 0) {
        const ssize_t w = ::send(fd, p, n, MSG_NOSIGNAL);
        if (w < 0) { if (errno == EINTR) continue; return false; }
        p += static_cast<size_t>(w); n -= static_cast<size_t>(w);
    }
    return true;
}

} // namespace

int main(int argc, char** argv) {
    Args args;
    std::string err;
    if (!args.parse(argc, argv,
                    {"bind", "port", "clients", "prelude", "rate", "file", "loop", "timing", "instruments", "count",
                     "mix", "gap-every", "heartbeat-ms"},
                    {"help", "synthetic"}, err)) {
        std::fprintf(stderr, "error: %s\n", err.c_str());
        usage();
        return 2;
    }
    if (args.has("help")) { usage(); return 0; }
    Opts o;
    o.bind = args.str("bind", o.bind);
    o.file = args.str("file", "");
    o.prelude = args.str("prelude", "dbn");
    o.timing = args.str("timing", "none");
    o.synthetic = args.has("synthetic");
    if (!args.u64("port", 0, o.port, err) || !args.u64("clients", 0, o.clients, err) || !args.u64("rate", 0, o.rate, err) ||
        !args.u64("loop", 1, o.loop, err) || !args.u64("instruments", 10, o.instruments, err) ||
        !args.u64("count", 0, o.count, err) || !args.u64("gap-every", 0, o.gap_every, err) ||
        !args.u64("heartbeat-ms", 0, o.heartbeat_ms, err)) {
        std::fprintf(stderr, "error: %s\n", err.c_str());
        return 2;
    }
    if (args.has("mix") && !parse_mix(args.str("mix", ""), o, err)) { std::fprintf(stderr, "error: %s\n", err.c_str()); return 2; }
    if (o.port == 0 || o.port > 65535 || (o.file.empty() == !o.synthetic) || o.instruments == 0 ||
        (o.prelude != "dbn" && o.prelude != "none") || (o.timing != "none" && o.timing != "file")) {
        std::fprintf(stderr, "error: need --port and exactly one of --file / --synthetic\n");
        usage();
        return 2;
    }

    DbnFile file;
    if (!o.file.empty() && !file.load(o.file, err)) { std::fprintf(stderr, "error: %s\n", err.c_str()); return 2; }

    std::signal(SIGINT, on_sigint);
    std::signal(SIGTERM, on_sigint);

    const int lfd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (lfd < 0) { std::perror("socket"); return 1; }
    int one = 1;
    ::setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(static_cast<uint16_t>(o.port));
    if (inet_pton(AF_INET, o.bind.c_str(), &a.sin_addr) != 1) { std::fprintf(stderr, "error: bad --bind\n"); return 2; }
    if (::bind(lfd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) < 0 || ::listen(lfd, 4) < 0) { std::perror("bind/listen"); return 1; }
    std::fprintf(stderr, "ft_dbn_sim: listening on %s:%llu (%s)\n", o.bind.c_str(), static_cast<unsigned long long>(o.port),
                 o.synthetic ? "synthetic" : o.file.c_str());

    uint64_t served = 0, total_records = 0, total_bytes = 0;
    const std::vector<uint8_t> prelude = o.synthetic ? synthetic_prelude()
                                                     : std::vector<uint8_t>(file.bytes.begin(), file.bytes.begin() + static_cast<long>(file.records_off));
    while (!g_stop && (o.clients == 0 || served < o.clients)) {
        pollfd pfd{lfd, POLLIN, 0};
        if (::poll(&pfd, 1, 200) <= 0) continue;
        const int cfd = ::accept(lfd, nullptr, nullptr);
        if (cfd < 0) continue;
        ::setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        ++served;
        uint64_t records = 0, bytes = 0;
        bool alive = true;
        if (o.prelude == "dbn") { alive = send_all(cfd, prelude.data(), prelude.size()); bytes += prelude.size(); }

        Pacer pacer(o.rate ? 1000000000ull / o.rate : 0);
        if (o.synthetic) {
            Synth syn(o);
            const uint64_t hb_ns = o.heartbeat_ms * 1000000ull;
            uint64_t next_hb = mono_ns() + hb_ns;
            for (uint64_t k = 1; alive && !g_stop && (o.count == 0 || k <= o.count); ++k) {
                pacer.wait(g_stop);
                if (hb_ns && mono_ns() >= next_hb) {
                    const auto& hb = syn.heartbeat();
                    alive = send_all(cfd, hb.data(), hb.size()); bytes += hb.size(); ++records;
                    next_hb = mono_ns() + hb_ns;
                }
                const auto& r = syn.build(k);
                alive = alive && send_all(cfd, r.data(), r.size());
                bytes += r.size(); ++records;
            }
        } else {
            for (uint64_t pass = 0; alive && !g_stop && (o.loop == 0 || pass < o.loop); ++pass) {
                uint64_t prev_ts = 0;
                for (size_t i = 0; i < file.recs.size() && alive && !g_stop; ++i) {
                    if (o.timing == "file") {
                        const uint64_t ts = file.ts_event(i);
                        if (prev_ts && ts > prev_ts) {
                            uint64_t d = ts - prev_ts;
                            if (d > 1000000000ull) d = 1000000000ull;
                            std::this_thread::sleep_for(std::chrono::nanoseconds(d));
                        }
                        prev_ts = ts;
                    } else {
                        pacer.wait(g_stop);
                    }
                    const auto [off, len] = file.recs[i];
                    alive = send_all(cfd, file.bytes.data() + off, len);
                    bytes += len; ++records;
                }
            }
        }
        ::close(cfd);
        total_records += records; total_bytes += bytes;
        std::fprintf(stderr, "ft_dbn_sim: client %llu done: records=%llu bytes=%llu%s\n", static_cast<unsigned long long>(served),
                     static_cast<unsigned long long>(records), static_cast<unsigned long long>(bytes),
                     alive ? "" : " (peer went away)");
    }
    ::close(lfd);
    std::printf("served: clients=%llu records=%llu bytes=%llu\n", static_cast<unsigned long long>(served),
                static_cast<unsigned long long>(total_records), static_cast<unsigned long long>(total_bytes));
    return 0;
}
