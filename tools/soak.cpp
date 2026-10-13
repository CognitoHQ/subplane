// subplane soak: shared-memory ring chaos test. One binary, three roles.
//
//   --role writer  --path P [--capacity-mb 4] [--max-payload 512] [--rate msgs/s|0] [--seconds S] [--crash-after S2]
//   --role reader  --path P [--seconds S] [--expect-restart]
//   --role harness [--path P] [--readers 4] [--seconds 2] [--rate 500000]
//
// Records: header.sequence is monotonic per writer run (starts at 1); payload =
// 4-byte CRC32 over (sequence LE, payload[4..]) followed by random bytes. The
// first record on every ring is AGENT_RESTARTED. A crashing writer (_exit
// without close()) leaves the ring ACTIVE with a stale heartbeat; readers must
// see WriterGone and then pick up the restarted writer's new epoch.
// "<pid>" in --path is replaced by the process id of the harness (or caller).
// Exit codes: 0 pass, 1 verification failure, 2 bad arguments, 3 restart not observed.

#include <sys/wait.h>
#include <unistd.h>

#include <csignal>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "commands.h"
#include "subplane/client/shm_reader.h"
#include "subplane/core/clock.h"
#include "subplane/fanout/ring_writer.h"
#include "subplane/shm/control_events.h"
#include "tool_common.h"

namespace {

using namespace subplane;
using sptool::Args;

void usage() {
    std::fputs(
        "usage: subplane soak --role writer|reader|harness [options]\n"
        "  writer : --path P [--capacity-mb 4] [--max-payload 512] [--rate msgs/s|0] [--seconds S] [--crash-after S2]\n"
        "  reader : --path P [--seconds S] [--expect-restart]\n"
        "  harness: [--path P] [--readers 4] [--seconds 2] [--rate 500000]\n"
        "  '<pid>' in --path is replaced by the process id. exit: 0 pass, 1 fail, 2 bad args, 3 no restart seen\n",
        stderr);
}

uint32_t crc32(const uint8_t* p, size_t n, uint32_t crc = 0) {
    static const auto table = [] {
        struct T { uint32_t v[256]; } t{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) c = (c & 1u) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
            t.v[i] = c;
        }
        return t;
    }();
    crc = ~crc;
    for (size_t i = 0; i < n; ++i) crc = table.v[(crc ^ p[i]) & 0xFFu] ^ (crc >> 8);
    return ~crc;
}

// CRC over (sequence, payload bytes after the crc field).
uint32_t record_crc(uint64_t seq, const uint8_t* payload, uint32_t len) {
    uint8_t s[8];
    std::memcpy(s, &seq, 8);
    return crc32(payload + 4, len - 4, crc32(s, 8));
}

std::string subst_pid(std::string p) {
    const std::string tag = "<pid>";
    for (size_t i = p.find(tag); i != std::string::npos; i = p.find(tag)) p.replace(i, tag.size(), std::to_string(::getpid()));
    return p;
}

uint64_t xs(uint64_t& s) { s ^= s << 13; s ^= s >> 7; s ^= s << 17; return s; }

// ---------------------------------------------------------------- writer

int run_writer(const Args& a, std::string& err) {
    uint64_t cap_mb = 4, max_payload = 512, rate = 0, seconds = 2, crash_after = 0;
    if (!a.u64("capacity-mb", 4, cap_mb, err) || !a.u64("max-payload", 512, max_payload, err) ||
        !a.u64("rate", 0, rate, err) || !a.u64("seconds", 2, seconds, err) || !a.u64("crash-after", 0, crash_after, err))
        return 2;
    const std::string path = subst_pid(a.str("path", ""));
    if (path.empty() || max_payload < 32 || cap_mb == 0) { err = "--path required, --max-payload >= 32, --capacity-mb >= 1"; return 2; }

    RingWriter w;
    RingConfig rc;
    rc.capacity_bytes = cap_mb << 20;
    rc.max_payload = static_cast<uint32_t>(max_payload);
    rc.session_name = "soak";
    const uint64_t epoch = now_ns();
    if (!w.open(rc, path, epoch, err)) return 1;
    w.heartbeat(now_ns());
    AgentRestartedEvent ev{epoch, 0};
    w.publish_control(ControlCode::AGENT_RESTARTED, 0, now_ns(), &ev, sizeof ev);

    std::vector<uint8_t> buf(max_payload);
    uint64_t rng = epoch | 1u, seq = 0;
    const uint64_t t0 = now_mono_ns();
    const uint64_t end = t0 + seconds * 1'000'000'000ull;
    const uint64_t crash_at = crash_after ? t0 + crash_after * 1'000'000'000ull : 0;
    const uint64_t interval = rate ? 1'000'000'000ull / rate : 0;
    uint64_t next_send = t0, next_hb = t0;
    for (;;) {
        const uint64_t now = now_mono_ns();
        if (crash_at && now >= crash_at) _exit(0); // INTENT: simulate kill -9; no close(), heartbeat goes stale.
        if (now >= end) break;
        if (now >= next_hb) { w.heartbeat(now_ns()); next_hb = now + 1'000'000; }
        if (interval) {
            if (now < next_send) continue;
            next_send += interval;
        }
        const auto len = static_cast<uint32_t>(32 + xs(rng) % (max_payload - 32 + 1));
        for (uint32_t i = 4; i < len; i += 8) {
            const uint64_t r = xs(rng);
            std::memcpy(buf.data() + i, &r, std::min<uint32_t>(8, len - i));
        }
        ++seq;
        const uint32_t crc = record_crc(seq, buf.data(), len);
        std::memcpy(buf.data(), &crc, 4);
        TickHeader h{};
        h.sequence = seq;
        h.ingest_ts = now_ns();
        h.template_id = 1;
        h.msg_type = static_cast<uint8_t>(MsgType::TRADE);
        h.kind = static_cast<uint8_t>(Kind::DATA);
        h.wire_format = static_cast<uint8_t>(WireFormat::SBE);
        if (!w.publish(h, buf.data(), len)) { err = "publish failed"; return 1; }
    }
    w.close();
    std::printf("writer epoch=%llu records=%llu\n", static_cast<unsigned long long>(epoch), static_cast<unsigned long long>(seq));
    return 0;
}

// ---------------------------------------------------------------- reader

struct ReaderStats {
    uint64_t records = 0, crc_fail = 0, seq_err = 0, gaps = 0, overruns = 0, lapped = 0;
    uint64_t gone = 0, restarts = 0, first_bad = 0, epochs = 0;
};

int run_reader(const Args& a, std::string& err) {
    uint64_t seconds = 2;
    if (!a.u64("seconds", 2, seconds, err)) return 2;
    const std::string path = subst_pid(a.str("path", ""));
    if (path.empty()) { err = "--path required"; return 2; }
    const bool expect_restart = a.has("expect-restart");

    ReaderStats st;
    ShmReader rd;
    const uint64_t deadline = now_mono_ns() + seconds * 1'000'000'000ull;
    uint64_t last_epoch = 0, last_seq = 0, last_live_check = 0;
    bool first_poll = true, gap_ok = true, done = false;

    while (!done && now_mono_ns() < deadline) {
        if (!rd.is_open()) {
            // Attach (retry until the file appears); a re-attach must land on a new epoch.
            if (!rd.open(path) || (last_epoch != 0 && rd.epoch() == last_epoch)) {
                rd.close();
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            last_epoch = rd.epoch();
            ++st.epochs;
            last_seq = 0;
            first_poll = true;
            gap_ok = false;
        }
        RecordView v;
        for (int burst = 0; burst < 256; ++burst) {
            const ReaderStatus s = rd.poll(v);
            if (s == ReaderStatus::Ok) {
                const bool was_first = first_poll;
                first_poll = false;
                const uint8_t kind = v.hdr->kind, mt = v.hdr->msg_type;
                const uint64_t seq = v.hdr->sequence;
                if (kind == static_cast<uint8_t>(Kind::DATA)) {
                    bool crc_ok = v.payload_len >= 4;
                    if (crc_ok) {
                        uint32_t want;
                        std::memcpy(&want, v.payload, 4);
                        crc_ok = want == record_crc(seq, v.payload, v.payload_len);
                    }
                    if (!v.valid()) { // lapped mid-read: contents unreliable, resume after the implied overrun
                        ++st.lapped;
                        gap_ok = true;
                        continue;
                    }
                    ++st.records;
                    if (!crc_ok) ++st.crc_fail;
                    if (seq <= last_seq) ++st.seq_err;
                    else if (seq != last_seq + 1) {
                        if (gap_ok) ++st.gaps; else ++st.seq_err;
                    }
                    if (seq > last_seq) last_seq = seq;
                    gap_ok = false;
                } else if (was_first && !(kind == static_cast<uint8_t>(Kind::AGENT_CONTROL) &&
                                          mt == static_cast<uint8_t>(ControlCode::AGENT_RESTARTED))) {
                    ++st.first_bad;
                }
                if (was_first && kind == static_cast<uint8_t>(Kind::AGENT_CONTROL) &&
                    mt == static_cast<uint8_t>(ControlCode::AGENT_RESTARTED) && v.valid())
                    ++st.restarts;
                if (was_first && kind == static_cast<uint8_t>(Kind::DATA)) ++st.first_bad;
                continue;
            }
            if (s == ReaderStatus::Overrun) {
                ++st.overruns;
                gap_ok = true;
                if (first_poll) first_poll = false; // attached after the writer lapped pos 0: nothing to assert
                continue;
            }
            if (s == ReaderStatus::Closed) { done = true; break; }
            // Empty: caught up. Check liveness at ~1 kHz-ish, only once the writer has heartbeated.
            const uint64_t now = now_mono_ns();
            if (now - last_live_check > 5'000'000) {
                last_live_check = now;
                if (rd.header().heartbeat_ns.load(std::memory_order_relaxed) != 0 &&
                    rd.liveness(200'000'000ull, now_ns()) == ReaderStatus::WriterGone) {
                    ++st.gone;
                    if (!expect_restart) { done = true; break; }
                    rd.close(); // wait for a new epoch
                }
            }
            if (rd.is_open()) std::this_thread::yield();
            break;
        }
        // A Closed ring in expect-restart mode is only final once a restart was seen.
        if (done && expect_restart && st.epochs < 2) { done = false; rd.close(); }
    }

    const bool restart_ok = !expect_restart || (st.gone >= 1 && st.epochs >= 2 && st.restarts >= 1);
    std::printf("reader: records=%llu crc_fail=%llu seq_err=%llu gaps_after_overrun=%llu overruns=%llu lapped=%llu "
                "writer_gone=%llu epochs=%llu agent_restarted=%llu first_record_bad=%llu\n",
                (unsigned long long)st.records, (unsigned long long)st.crc_fail, (unsigned long long)st.seq_err,
                (unsigned long long)st.gaps, (unsigned long long)st.overruns, (unsigned long long)st.lapped,
                (unsigned long long)st.gone, (unsigned long long)st.epochs, (unsigned long long)st.restarts,
                (unsigned long long)st.first_bad);
    if (st.crc_fail || st.seq_err || st.first_bad) return 1;
    if (st.records == 0) { err = "no records observed"; return 1; }
    return restart_ok ? 0 : 3;
}

// ---------------------------------------------------------------- harness

pid_t spawn(const std::vector<std::string>& args) {
    const pid_t pid = ::fork();
    if (pid != 0) return pid;
    std::vector<char*> argv;
    for (const auto& s : args) argv.push_back(const_cast<char*>(s.c_str()));
    argv.push_back(nullptr);
    ::execv("/proc/self/exe", argv.data());
    _exit(127);
}

int wait_child(pid_t pid, uint64_t deadline_mono) {
    int status = 0;
    for (;;) {
        const pid_t r = ::waitpid(pid, &status, WNOHANG);
        if (r == pid) return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
        if (r < 0) return 127;
        if (now_mono_ns() > deadline_mono) { ::kill(pid, SIGKILL); ::waitpid(pid, &status, 0); return 124; }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}

int run_harness(const Args& a, std::string& err) {
    uint64_t nreaders = 4, seconds = 2, rate = 500000;
    if (!a.u64("readers", 4, nreaders, err) || !a.u64("seconds", 2, seconds, err) || !a.u64("rate", 500000, rate, err)) return 2;
    if (nreaders == 0 || nreaders > 64 || seconds < 2) { err = "--readers 1..64, --seconds >= 2"; return 2; }
    const std::string path = subst_pid(a.str("path", "/dev/shm/subplane.soak.<pid>.ring"));
    ::unlink(path.c_str());
    ::unlink((path + ".tmp").c_str());

    const uint64_t cap = now_mono_ns() + (seconds + 6) * 1'000'000'000ull;
    const std::string self = "subplane", sub = "soak", sec = std::to_string(seconds + 4);
    std::vector<pid_t> readers;
    for (uint64_t i = 0; i < nreaders; ++i)
        readers.push_back(spawn({self, sub, "--role", "reader", "--path", path, "--seconds", sec, "--expect-restart"}));

    const std::string rt = std::to_string(rate);
    int rc = 0;
    // Incarnation 1 crashes after 1 s without closing the ring.
    pid_t w = spawn({self, sub, "--role", "writer", "--path", path, "--seconds", sec, "--rate", rt, "--crash-after", "1"});
    int wr = wait_child(w, cap);
    std::printf("harness: writer1 exit=%d (crash simulated)\n", wr);
    if (wr != 0) rc = 1;
    std::this_thread::sleep_for(std::chrono::milliseconds(500)); // > 200 ms liveness window
    w = spawn({self, sub, "--role", "writer", "--path", path, "--seconds", std::to_string(seconds - 1), "--rate", rt});
    wr = wait_child(w, cap);
    std::printf("harness: writer2 exit=%d\n", wr);
    if (wr != 0) rc = 1;

    int passed = 0;
    for (size_t i = 0; i < readers.size(); ++i) {
        const int r = wait_child(readers[i], cap);
        std::printf("harness: reader%zu exit=%d\n", i, r);
        if (r == 0) ++passed; else rc = 1;
    }
    ::unlink(path.c_str());
    ::unlink((path + ".tmp").c_str());
    std::printf("harness: %d/%llu readers passed -> %s\n", passed, (unsigned long long)nreaders, rc == 0 ? "PASS" : "FAIL");
    return rc;
}

} // namespace

int cmd_soak(int argc, char** argv) {
    Args a;
    std::string err;
    if (!a.parse(argc, argv,
                 {"role", "path", "capacity-mb", "max-payload", "rate", "seconds", "crash-after", "readers"},
                 {"expect-restart", "help"}, err)) {
        std::fprintf(stderr, "error: %s\n", err.c_str());
        usage();
        return 2;
    }
    if (a.has("help")) { usage(); return 0; }
    const std::string role = a.str("role", "");
    int rc;
    if (role == "writer") rc = run_writer(a, err);
    else if (role == "reader") rc = run_reader(a, err);
    else if (role == "harness") rc = run_harness(a, err);
    else { err = "--role writer|reader|harness is required"; rc = 2; }
    if (rc != 0 && !err.empty()) std::fprintf(stderr, "error: %s\n", err.c_str());
    if (rc == 2) usage();
    return rc;
}
