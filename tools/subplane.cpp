// subplane: the operator CLI. One binary, one subcommand per tool.
//
//   subplane <command> [options]      subplane <command> --help

#include "commands.h"

#include <cstdio>
#include <cstring>

namespace {

struct Command { const char* name; int (*fn)(int, char**); const char* summary; };

constexpr Command kCommands[] = {
    {"probe",     &cmd_probe,     "validate a framing block against live packets"},
    {"tail",      &cmd_tail,      "dump records from a shared-memory ring"},
    {"schema",    &cmd_schema,    "generate typed C++ / NumPy / q views from an SBE schema"},
    {"send",      &cmd_send,      "synthetic SBE publisher over UDP or multicast"},
    {"replay",    &cmd_replay,    "replay a pcap over UDP, or benchmark the pipeline in-process"},
    {"dbn-sim",   &cmd_dbn_sim,   "serve DBN over TCP like Databento's live gateway"},
#if SUBPLANE_WITH_AERON
    {"aeron-pub", &cmd_aeron_pub, "publish canonical messages over Aeron"},
#endif
    {"soak",      &cmd_soak,      "shared-memory ring chaos test"},
};

void usage() {
    std::fputs("usage: subplane <command> [options]\n\ncommands:\n", stderr);
    for (const auto& c : kCommands) std::fprintf(stderr, "  %-10s %s\n", c.name, c.summary);
    std::fputs("\nsubplane <command> --help for the command's options\n", stderr);
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2 || std::strcmp(argv[1], "--help") == 0 || std::strcmp(argv[1], "-h") == 0 || std::strcmp(argv[1], "help") == 0) {
        usage();
        return argc < 2 ? 2 : 0;
    }
    for (const auto& c : kCommands) {
        if (std::strcmp(argv[1], c.name) == 0) return c.fn(argc - 1, argv + 1);
    }
    std::fprintf(stderr, "subplane: unknown command '%s'\n\n", argv[1]);
    usage();
    return 2;
}
