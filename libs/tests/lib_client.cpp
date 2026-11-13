// A subplane-lib client process for valkey_e2e.sh: several of these share one
// valkey-server, as the processes of a site would.
//
//   lib_client rate  <port> <prefix> <site_rate> <client_rate> <lease> <floor_rate> <seconds>
//       spends tokens as fast as its Limiter allows; prints spent, granted, floor tokens
//   lib_client elect <port> <prefix> <group> <ttl_ms> <seconds>
//       one Candidate; prints each change as "<ms> <change> epoch=<n>"
#include "subplane/lib/elect.h"
#include "subplane/lib/rate.h"
#include "subplane/lib/runner.h"
#include "subplane/lib/valkey_store.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <unistd.h>

using namespace subplane;

namespace {

constexpr uint64_t kNsPerMs = 1'000'000;
constexpr uint64_t kMsPerS = 1000;
constexpr uint32_t kRunnerTickMs = 1;
constexpr uint32_t kElectTickMs = 10;
constexpr uint64_t kSiteBurstDivisor = 10;  // site burst: a tenth of a second at the site rate

uint64_t arg(char** argv, int i) { return std::strtoull(argv[i], nullptr, 10); }

int rate_main(lib::ValkeyStore& store, char** argv) {
    const uint64_t site = arg(argv, 4);
    const uint64_t mine = arg(argv, 5);
    rate::Config c;
    c.path = {lib::Bucket{"site", site, site / kSiteBurstDivisor},
              lib::Bucket{"site/client." + std::to_string(::getpid()), mine, mine / kSiteBurstDivisor}};
    c.lease = arg(argv, 6);
    c.floor_rate = arg(argv, 7);
    const uint64_t end = lib::now_ns() + arg(argv, 8) * kMsPerS * kNsPerMs;
    rate::Limiter l(c);
    lib::Runner r(store, kRunnerTickMs);
    r.add(l);
    r.start();
    uint64_t spent = 0;
    while (lib::now_ns() < end) {
        if (l.try_acquire(1)) ++spent;
        else std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    r.stop();
    std::printf("spent=%llu granted=%llu floor_tokens=%llu store_errors=%llu\n", static_cast<unsigned long long>(spent),
                static_cast<unsigned long long>(l.stats().granted.load()),
                static_cast<unsigned long long>(l.stats().floor_tokens.load()),
                static_cast<unsigned long long>(l.stats().store_errors.load()));
    return 0;
}

int elect_main(lib::ValkeyStore& store, char** argv) {
    elect::Config c;
    c.group = argv[4];
    c.ttl_ms = static_cast<uint32_t>(arg(argv, 5));
    const uint64_t start = lib::now_ns();
    const uint64_t end = start + arg(argv, 6) * kMsPerS * kNsPerMs;
    elect::Candidate cand(c);
    auto report = [&](elect::Change ch) {
        if (ch == elect::Change::NONE) return;
        std::printf("%llu %s epoch=%llu\n", static_cast<unsigned long long>((lib::now_ns() - start) / kNsPerMs),
                    elect::to_string(ch), static_cast<unsigned long long>(cand.epoch()));
    };
    while (lib::now_ns() < end) {
        report(cand.step(store, lib::now_ns()));
        std::this_thread::sleep_for(std::chrono::milliseconds(kElectTickMs));
    }
    report(cand.stop(store));
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    const bool rate = argc == 9 && std::strcmp(argv[1], "rate") == 0;
    const bool elect = argc == 7 && std::strcmp(argv[1], "elect") == 0;
    if (!rate && !elect) {
        std::fprintf(stderr, "usage: lib_client rate <port> <prefix> <site_rate> <client_rate> <lease> <floor_rate> <seconds>\n"
                             "       lib_client elect <port> <prefix> <group> <ttl_ms> <seconds>\n");
        return 2;
    }
    lib::ValkeyOptions o;
    o.port = static_cast<int>(arg(argv, 2));
    o.prefix = argv[3];
    lib::ValkeyStore store(o);
    return rate ? rate_main(store, argv) : elect_main(store, argv);
}
