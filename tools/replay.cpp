// subplane replay: pcap/pcapng replay and in-process pipeline benchmark.
//
//   subplane replay --pcap <file> --port N [--host H] [--rate pps|0] [--timing pcap] [--loop N]
//   subplane replay --bench --config <config.yaml> [--packets N] [--per-packet K] ...
//
// Bench mode builds partitions[0] of the config with an in-memory transport,
// drives Pipeline::run_once() on the calling thread and reports the pipeline's
// own recv_to_publish histogram. The recv timestamp is taken inside poll(), so
// for a batch it covers frame + decode + filter + publish of the whole batch.
// Exit codes: 0 ok, 1 runtime failure, 2 bad arguments.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sched.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "commands.h"
#include "subplane/core/clock.h"
#include "subplane/core/histogram.h"
#include "subplane/pipeline/lvc.h"
#include "subplane/pipeline/partition.h"
#include "subplane/pipeline/pipeline.h"
#include "subplane/pipeline/session_manager.h"
#include "subplane/record/pcapng.h"
#include "subplane/symbol_table.h"
#include "packet_gen.h"

namespace {

using sptool::Args;

volatile std::sig_atomic_t g_stop = 0;
void on_sigint(int) { g_stop = 1; }

void usage() {
    std::fputs(
        "usage:\n"
        "  subplane replay --pcap <file> --port <n> [--host 127.0.0.1] [--rate pps|0] [--timing pcap] [--loop N]\n"
        "      Replays UDP payloads from a pcap or pcapng file. --rate 0 = as fast as possible (default);\n"
        "      --timing pcap reproduces capture gaps (each capped at 1s). --loop 0 = forever.\n"
        "  subplane replay --bench --config <config.yaml> [options]\n"
        "      --packets N=200000   measured packets (a 20k-packet warm-up precedes them)\n"
        "      --per-packet K=3     messages per datagram\n"
        "      --symbols N=100      distinct SecurityIDs\n"
        "      --mix trade=70,quote=25,book=5\n"
        "      --sessions K=1       sessions attached for the measured run\n"
        "      --all-symbols | --symbols-per-session M   session symbol filter (default all)\n"
        "      --batch N=32         packets per transport poll (max 64)\n"
        "      --cpu N              pin the calling thread\n"
        "      --json               print a JSON object instead of the table\n"
        "exit: 0 ok, 1 runtime failure, 2 bad arguments\n",
        stderr);
}

uint64_t mono_ns() { return subplane::now_mono_ns(); }

// ---------------------------------------------------------------- pcap replay

struct PcapPacket {
    size_t   off;     // payload offset in the file buffer
    uint32_t len;
    uint64_t ts_ns;
};

struct PcapStats {
    uint64_t records = 0, udp = 0, non_udp = 0, ipv6 = 0, fragments = 0, truncated = 0;
};

// Classic pcap or pcapng (subplane recordings are pcapng): every IPv4 UDP payload, in file order.
// A file cut short replays the packets before the cut and counts it as truncated.
bool load_pcap(const std::string& path, subplane::record::MappedFile& file, std::vector<PcapPacket>& out,
               PcapStats& st, std::string& err) {
    using namespace subplane::record;
    if (!file.open(path, err)) return false;
    PcapReader rd;
    if (!rd.open(file.data(), file.size(), err)) return false;
    PcapItem it;
    PcapStatus s;
    while ((s = rd.next(it)) == PcapStatus::Ok) {
        if (it.kind != PcapItem::PACKET) continue;
        ++st.records;
        uint32_t off = 0, len = 0;
        switch (udp_payload(it.linktype, it.data, it.caplen, off, len)) {
            case UdpExtract::OK:
                out.push_back({static_cast<size_t>(it.data + off - file.data()), len, it.ts_ns});
                ++st.udp;
                break;
            case UdpExtract::IPV6: ++st.ipv6; break;
            case UdpExtract::FRAGMENT: ++st.fragments; break;
            case UdpExtract::TRUNCATED: ++st.truncated; break;
            case UdpExtract::NOT_UDP: ++st.non_udp; break;
        }
    }
    if (s == PcapStatus::Error) {
        ++st.truncated;
        std::fprintf(stderr, "pcap: %s; replaying the %zu packets before it\n", rd.error().c_str(), out.size());
    }
    return true;
}

int run_pcap(const Args& args, std::string& err) {
    uint64_t port = 0, rate = 0, loops = 1;
    if (!args.u64("port", 0, port, err) || !args.u64("rate", 0, rate, err) || !args.u64("loop", 1, loops, err)) return 2;
    if (port == 0 || port > 65535) { err = "--port (1-65535) is required"; return 2; }
    const std::string timing = args.str("timing", "");
    if (!timing.empty() && timing != "pcap") { err = "--timing accepts only 'pcap'"; return 2; }
    const std::string host = args.str("host", "127.0.0.1");

    subplane::record::MappedFile buf;
    std::vector<PcapPacket> pk;
    PcapStats st;
    if (!load_pcap(args.str("pcap", ""), buf, pk, st, err)) return 1;

    sockaddr_in dst{};
    dst.sin_family = AF_INET;
    dst.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, host.c_str(), &dst.sin_addr) != 1) { err = "bad --host address"; return 2; }
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) { err = std::string("socket: ") + std::strerror(errno); return 1; }
    if ((ntohl(dst.sin_addr.s_addr) >> 28) == 0xE) {
        const unsigned char ttl = 1, lp = 1;
        setsockopt(fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof ttl);
        setsockopt(fd, IPPROTO_IP, IP_MULTICAST_LOOP, &lp, sizeof lp);
    }
    std::signal(SIGINT, on_sigint);
    std::signal(SIGTERM, on_sigint);

    const bool pcap_timing = timing == "pcap";
    const uint64_t interval = (!pcap_timing && rate) ? 1'000'000'000ull / rate : 0;
    uint64_t sent = 0, bytes = 0, send_errors = 0;
    const uint64_t t0 = mono_ns();
    uint64_t deadline = t0;
    auto wait_until = [&](uint64_t target) {
        for (;;) {
            const uint64_t now = mono_ns();
            if (now >= target || g_stop) return;
            if (target - now > 100'000) std::this_thread::sleep_for(std::chrono::nanoseconds(target - now - 50'000));
        }
    };
    for (uint64_t l = 0; !g_stop && (loops == 0 || l < loops); ++l) {
        for (size_t i = 0; i < pk.size() && !g_stop; ++i) {
            if (pcap_timing && i > 0) {
                const uint64_t gap = pk[i].ts_ns > pk[i - 1].ts_ns ? pk[i].ts_ns - pk[i - 1].ts_ns : 0;
                deadline += std::min<uint64_t>(gap, 1'000'000'000ull);
                wait_until(deadline);
            } else if (interval) {
                deadline += interval;
                wait_until(deadline);
            }
            bool ok = false;
            for (int tries = 0; tries < 100 && !ok; ++tries) {
                ok = ::sendto(fd, buf.data() + pk[i].off, pk[i].len, 0, reinterpret_cast<const sockaddr*>(&dst), sizeof dst) >= 0;
                if (!ok && errno != EAGAIN && errno != ENOBUFS && errno != EINTR) break;
            }
            if (ok) { ++sent; bytes += pk[i].len; } else { ++send_errors; }
        }
        if (pk.empty()) break;
    }
    ::close(fd);
    const double el = static_cast<double>(mono_ns() - t0) / 1e9;
    std::printf("replayed: sent=%llu bytes=%llu elapsed=%.3fs rate=%.0f pkt/s send_errors=%llu\n"
                "pcap: records=%llu udp=%llu skipped(non_udp=%llu ipv6=%llu fragments=%llu truncated=%llu)\n",
                static_cast<unsigned long long>(sent), static_cast<unsigned long long>(bytes), el,
                el > 0 ? static_cast<double>(sent) / el : 0.0, static_cast<unsigned long long>(send_errors),
                static_cast<unsigned long long>(st.records), static_cast<unsigned long long>(st.udp),
                static_cast<unsigned long long>(st.non_udp), static_cast<unsigned long long>(st.ipv6),
                static_cast<unsigned long long>(st.fragments), static_cast<unsigned long long>(st.truncated));
    return send_errors == 0 ? 0 : 1;
}

// ------------------------------------------------------------------- bench

// Serves pre-generated packets, up to `batch` per poll, stamping recv_ts at poll time.
class BenchTransport final : public subplane::Transport {
public:
    explicit BenchTransport(int batch) : batch_(batch) {}
    void load(std::vector<uint8_t>&& arena, std::vector<std::pair<size_t, uint32_t>>&& slices) {
        arena_ = std::move(arena);
        slices_ = std::move(slices);
        next_ = limit_ = 0;
    }
    void set_limit(size_t l) { limit_ = std::min(l, slices_.size()); }
    size_t remaining() const { return limit_ - next_; }

    bool open(std::string&) override { return true; }
    int poll(subplane::PacketBatch& out) noexcept override {
        const uint64_t ts = subplane::now_ns();
        int n = 0;
        while (n < batch_ && next_ < limit_) {
            const auto& s = slices_[next_++];
            out.pkts[n] = subplane::PacketView{arena_.data() + s.first, s.second, ts};
            ++n;
        }
        out.count = n;
        return n;
    }
    void close() noexcept override {}
    std::string describe() const override { return "bench"; }
    int fd() const noexcept override { return -1; }

private:
    int batch_;
    std::vector<uint8_t> arena_;
    std::vector<std::pair<size_t, uint32_t>> slices_;
    size_t next_ = 0, limit_ = 0;
};

struct BenchOpts {
    std::string config;
    uint64_t packets = 200000, warmup = 20000, sessions = 1, symbols_per_session = 0, batch = 32;
    sptool::GenOptions gen;
    int cpu = -1;
};

struct RunResult {
    std::string name;
    uint64_t sessions = 0, packets = 0, messages = 0, published = 0, elapsed_ns = 0;
    subplane::LogHistogram::Snapshot r2p; // measured-window delta
    uint64_t r2p_max = 0;
};

subplane::LogHistogram::Snapshot delta(const subplane::LogHistogram::Snapshot& a,
                                        const subplane::LogHistogram::Snapshot& b, uint64_t& max_out) {
    subplane::LogHistogram::Snapshot d;
    int hi = -1;
    for (unsigned i = 0; i < subplane::LogHistogram::kBuckets; ++i) {
        d.buckets[i] = b.buckets[i] - a.buckets[i];
        if (d.buckets[i]) hi = static_cast<int>(i);
    }
    d.count = b.count - a.count;
    d.sum = b.sum - a.sum;
    // The cumulative max may belong to the warm-up; bound it by the top occupied bucket.
    max_out = hi < 0 ? 0 : std::min(b.max, subplane::LogHistogram::bucket_upper_edge(static_cast<unsigned>(hi)));
    d.max = max_out;
    return d;
}

bool run_one(const BenchOpts& o, const YAML::Node& part_node, const std::string& base_dir,
             const sptool::FramingConfig& framing, uint64_t n_sessions, const std::string& name,
             RunResult& res, std::string& err) {
    using namespace subplane;
    subplane::PartitionConfig pcfg;
    if (!PartitionConfig::parse(part_node, 0, base_dir, pcfg, err)) return false;

    SymbolTable symbols(4096);
    Lvc lvc;
    if (!lvc.init(4096, {static_cast<uint8_t>(MsgType::TRADE), static_cast<uint8_t>(MsgType::QUOTE),
                         static_cast<uint8_t>(MsgType::BOOK_UPDATE)}, 512, err)) return false;
    Partition part;
    auto bt = std::make_unique<BenchTransport>(static_cast<int>(o.batch));
    BenchTransport* bench = bt.get();
    // INTENT: transport_override so the config's multicast group is never joined.
    if (!part.build(pcfg, err, std::move(bt)) || !part.open(err)) return false;

    // Pre-generate every packet so generation cost is outside the measurement.
    if (!part.schema()) { err = "--bench needs an sbe_routing partition (the generator encodes SBE); partition uses " + std::string(decoder_type_name(part.decoder_type())); return false; }
    sptool::GenOptions go = o.gen;
    sptool::Generator gen(*part.schema(), framing, go);
    if (!gen.init(err)) return false;
    const uint64_t total = o.warmup + o.packets;
    std::vector<uint8_t> arena;
    std::vector<std::pair<size_t, uint32_t>> slices;
    arena.reserve(static_cast<size_t>(total) * 256);
    slices.reserve(static_cast<size_t>(total));
    for (uint64_t k = 1; k <= total; ++k) {
        const uint32_t len = gen.build(k, false);
        if (len == 0) { err = "packet generation failed; reduce --per-packet"; return false; }
        slices.emplace_back(arena.size(), len);
        arena.insert(arena.end(), gen.data(), gen.data() + len);
    }
    bench->load(std::move(arena), std::move(slices));

    Pipeline pipe("bench", now_ns(), symbols, lvc);
    pipe.add_partition(&part);
    SessionManagerConfig smc;
    smc.ring_dir = "/dev/shm";
    smc.agent_name = "ftbench" + std::to_string(::getpid()) + name;
    smc.epoch = now_ns();
    smc.max_symbols = 4096;
    SessionManager sm(smc, symbols, lvc, {&part}, {&pipe});
    for (uint64_t i = 0; i < n_sessions; ++i) {
        SessionSpec s;
        s.name = "s" + std::to_string(i);
        s.ring_capacity_bytes = 16ull << 20;
        s.max_payload = 4096;
        if (o.symbols_per_session > 0) {
            for (uint64_t m = 0; m < o.symbols_per_session; ++m) s.symbols.push_back(std::to_string(1 + m % o.gen.symbols));
        } else {
            s.all_symbols = true;
        }
        if (!sm.create(s, err)) return false;
    }

    auto drain = [&](size_t upto) {
        bench->set_limit(upto);
        while (bench->remaining() > 0) pipe.run_once();
    };
    drain(static_cast<size_t>(o.warmup));

    const auto r0 = pipe.hist_recv_to_publish().snapshot();
    const auto c0 = part.counters.snapshot();
    const uint64_t t0 = mono_ns();
    drain(static_cast<size_t>(total));
    const uint64_t t1 = mono_ns();
    const auto c1 = part.counters.snapshot();
    res.name = name;
    res.sessions = n_sessions;
    res.elapsed_ns = t1 - t0;
    res.packets = c1.packets - c0.packets;
    res.messages = c1.messages - c0.messages;
    res.r2p = delta(r0, pipe.hist_recv_to_publish().snapshot(), res.r2p_max);
    for (const auto& st : sm.status()) res.published += st.published;
    sm.destroy_all();
    return true;
}

double per_s(uint64_t n, uint64_t ns) { return ns ? static_cast<double>(n) * 1e9 / static_cast<double>(ns) : 0.0; }
unsigned long long ull(uint64_t v) { return static_cast<unsigned long long>(v); }

void print_hist_json(std::string& o, const char* key, const subplane::LogHistogram::Snapshot& s, uint64_t mx) {
    char b[256];
    std::snprintf(b, sizeof b, "\"%s\":{\"count\":%llu,\"p50\":%llu,\"p90\":%llu,\"p99\":%llu,\"p999\":%llu,\"max\":%llu}",
                  key, ull(s.count), ull(subplane::percentile(s, 0.5)), ull(subplane::percentile(s, 0.9)),
                  ull(subplane::percentile(s, 0.99)), ull(subplane::percentile(s, 0.999)), ull(mx));
    o += b;
}

std::string run_json(const RunResult& r) {
    char b[512];
    std::snprintf(b, sizeof b,
                  "{\"name\":\"%s\",\"sessions\":%llu,\"packets\":%llu,\"messages\":%llu,\"published\":%llu,"
                  "\"elapsed_ns\":%llu,\"packets_per_s\":%.0f,\"messages_per_s\":%.0f,\"ns_per_message\":%.1f,",
                  r.name.c_str(), ull(r.sessions), ull(r.packets), ull(r.messages), ull(r.published),
                  ull(r.elapsed_ns), per_s(r.packets, r.elapsed_ns), per_s(r.messages, r.elapsed_ns),
                  r.messages ? static_cast<double>(r.elapsed_ns) / static_cast<double>(r.messages) : 0.0);
    std::string o = b;
    print_hist_json(o, "recv_to_publish", r.r2p, r.r2p_max);
    o += "}";
    return o;
}

int run_bench(const Args& args, std::string& err) {
    BenchOpts o;
    o.config = args.str("config", "");
    if (o.config.empty()) { err = "--config is required with --bench"; return 2; }
    uint64_t per_packet = 3, symbols = 100, cpu = 0;
    if (!args.u64("packets", 200000, o.packets, err) || !args.u64("per-packet", 3, per_packet, err) ||
        !args.u64("symbols", 100, symbols, err) || !args.u64("sessions", 1, o.sessions, err) ||
        !args.u64("symbols-per-session", 0, o.symbols_per_session, err) || !args.u64("batch", 32, o.batch, err) ||
        !args.u64("cpu", 0, cpu, err)) return 2;
    if (o.packets == 0 || per_packet == 0 || symbols == 0 || o.batch == 0 || o.batch > 64 || o.sessions > 200) {
        err = "--packets/--per-packet/--symbols >= 1, --batch 1..64, --sessions <= 200";
        return 2;
    }
    if (args.has("all-symbols") && o.symbols_per_session > 0) { err = "--all-symbols and --symbols-per-session conflict"; return 2; }
    o.gen.per_packet = per_packet;
    o.gen.symbols = symbols;
    if (args.has("mix") && !sptool::parse_mix(args.str("mix", ""), o.gen, err)) return 2;
    if (args.has("cpu")) {
        o.cpu = static_cast<int>(cpu);
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(static_cast<size_t>(o.cpu), &set);
        if (pthread_setaffinity_np(pthread_self(), sizeof set, &set) != 0)
            std::fprintf(stderr, "warning: could not pin to cpu %d\n", o.cpu);
    }

    YAML::Node root;
    try {
        root = YAML::LoadFile(o.config);
    } catch (const YAML::Exception& e) {
        err = std::string("config: ") + e.what();
        return 1;
    }
    if (!root["partitions"] || !root["partitions"].IsSequence() || root["partitions"].size() == 0) {
        err = "config has no partitions";
        return 1;
    }
    const YAML::Node part_node = root["partitions"][0];
    sptool::FramingConfig framing;
    if (!sptool::load_framing(o.config, framing, err)) return 1;
    std::filesystem::path base = std::filesystem::absolute(o.config).parent_path();

    RunResult base_run, sess_run;
    if (!run_one(o, part_node, base.string(), framing, 0, "poll_only", base_run, err)) return 1;
    if (o.sessions > 0 && !run_one(o, part_node, base.string(), framing, o.sessions, "sessions", sess_run, err)) return 1;

    if (args.has("json")) {
        std::string j = "{\"packets\":" + std::to_string(o.packets) + ",\"per_packet\":" + std::to_string(per_packet) +
                        ",\"symbols\":" + std::to_string(symbols) + ",\"sessions\":" + std::to_string(o.sessions) +
                        ",\"poll_only\":" + run_json(base_run);
        if (o.sessions > 0) j += ",\"with_sessions\":" + run_json(sess_run);
        j += "}";
        std::puts(j.c_str());
        return 0;
    }
    std::printf("subplane replay bench: packets=%llu (+%llu warm-up) per_packet=%llu symbols=%llu sessions=%llu batch=%llu cpu=%d\n",
                ull(o.packets), ull(o.warmup), ull(per_packet), ull(symbols), ull(o.sessions), ull(o.batch), o.cpu);
    std::printf("%-10s %4s %10s %10s %9s | recv_to_publish ns (per batch): %7s %7s %7s %7s %8s\n", "run", "sess",
                "pkt/s", "msg/s", "ns/msg", "p50", "p90", "p99", "p99.9", "max");
    auto row = [](const RunResult& r) {
        std::printf("%-10s %4llu %10.0f %10.0f %9.1f | %38s %7llu %7llu %7llu %7llu %8llu\n", r.name.c_str(),
                    ull(r.sessions), per_s(r.packets, r.elapsed_ns), per_s(r.messages, r.elapsed_ns),
                    r.messages ? static_cast<double>(r.elapsed_ns) / static_cast<double>(r.messages) : 0.0, "",
                    ull(subplane::percentile(r.r2p, 0.5)), ull(subplane::percentile(r.r2p, 0.9)),
                    ull(subplane::percentile(r.r2p, 0.99)), ull(subplane::percentile(r.r2p, 0.999)), ull(r.r2p_max));
    };
    row(base_run);
    if (o.sessions > 0) {
        row(sess_run);
        const double per_sess = o.sessions && sess_run.messages
            ? (static_cast<double>(sess_run.elapsed_ns) - static_cast<double>(base_run.elapsed_ns)) /
              static_cast<double>(sess_run.messages) / static_cast<double>(o.sessions)
            : 0.0;
        std::printf("per-session publish cost: %.1f ns/msg/session; records published: %llu\n", per_sess, ull(sess_run.published));
    }
    std::puts("(percentiles are histogram bucket upper edges, <=25% relative error; max is bucket-bounded)");
    return 0;
}

} // namespace

int cmd_replay(int argc, char** argv) {
    Args args;
    std::string err;
    if (!args.parse(argc, argv,
                    {"pcap", "port", "host", "rate", "timing", "loop", "config", "packets", "per-packet", "symbols",
                     "mix", "sessions", "symbols-per-session", "batch", "cpu"},
                    {"bench", "all-symbols", "json", "help"}, err)) {
        std::fprintf(stderr, "error: %s\n", err.c_str());
        usage();
        return 2;
    }
    if (args.has("help")) { usage(); return 0; }
    int rc;
    if (args.has("bench")) rc = run_bench(args, err);
    else if (args.has("pcap")) rc = run_pcap(args, err);
    else { err = "one of --pcap or --bench is required"; rc = 2; }
    if (rc != 0 && !err.empty()) std::fprintf(stderr, "error: %s\n", err.c_str());
    if (rc == 2) usage();
    return rc;
}
