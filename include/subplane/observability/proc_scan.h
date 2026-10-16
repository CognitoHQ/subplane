#pragma once

// Finds the processes that map given files, from /proc/<pid>/maps, so the
// agent can name the apps reading its rings even when they never use the
// control socket (observability.md, "Consumers"). Cold path; a scan reads the
// maps of every process it may read.
//
// Limits: reading another user's maps needs root or CAP_SYS_PTRACE; those
// processes are counted in `unreadable` and missed. An agent in its own
// container sees only its pid namespace.

#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

namespace subplane::observability {

struct Mapper {
    int32_t     pid = 0;
    uint64_t    start_time = 0;  // clock ticks since boot (/proc/<pid>/stat field 22): (pid, start_time) is unique
    uint32_t    uid = 0;
    std::string comm;            // /proc/<pid>/comm
    std::string unit;            // the systemd unit or container scope from /proc/<pid>/cgroup, if any
    std::string path;            // the mapped file
    bool        deleted = false; // mapped after the file was unlinked: an old ring from a previous agent run
};

struct ScanResult {
    std::vector<Mapper> mappers; // one per (process, file)
    uint32_t processes = 0;      // processes looked at
    uint32_t unreadable = 0;     // whose maps could not be read
};

// `files`: exact paths to look for. `self`: a pid to skip (the agent itself).
// `proc_root` is "/proc" except in tests.
ScanResult scan_mappers(const std::unordered_set<std::string>& files, int32_t self, const std::string& proc_root = "/proc");

// The parts of /proc/<pid>/... the scan uses, exposed for tests and reuse.
bool read_identity(const std::string& proc_root, int32_t pid, Mapper& out);

} // namespace subplane::observability
