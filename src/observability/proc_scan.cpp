#include "subplane/observability/proc_scan.h"

#include <dirent.h>

#include <cstdlib>
#include <fstream>
#include <sstream>

namespace subplane::observability {

namespace {

bool read_file(const std::string& path, std::string& out) {
    std::ifstream f(path);
    if (!f) return false;
    std::ostringstream ss;
    ss << f.rdbuf();
    out = ss.str();
    return !f.bad();
}

// The unit from a cgroup v2 (or v1) path: the last component ending in
// .service or .scope, e.g. "mm-engine.service", "docker-1a2b....scope".
std::string unit_of(const std::string& cgroup) {
    std::string unit;
    std::istringstream in(cgroup);
    for (std::string line; std::getline(in, line);) {
        const size_t colon = line.rfind(':');
        std::string path = colon == std::string::npos ? line : line.substr(colon + 1);
        while (!path.empty()) {
            const size_t slash = path.rfind('/');
            const std::string last = path.substr(slash == std::string::npos ? 0 : slash + 1);
            const bool svc = last.size() > 8 && last.compare(last.size() - 8, 8, ".service") == 0;
            const bool scope = last.size() > 6 && last.compare(last.size() - 6, 6, ".scope") == 0;
            if ((svc || scope) && last.rfind("session-", 0) != 0 && last != "init.scope") return last;
            if (slash == std::string::npos) break;
            path.resize(slash);
        }
    }
    return unit;
}

} // namespace

bool read_identity(const std::string& proc_root, int32_t pid, Mapper& out) {
    const std::string dir = proc_root + "/" + std::to_string(pid);
    std::string s;
    if (!read_file(dir + "/stat", s)) return false;
    // INTENT: comm in stat may contain spaces and ')': fields after it are
    // counted from the LAST ')'. starttime is field 22, the 20th after it.
    const size_t rp = s.rfind(')');
    if (rp == std::string::npos) return false;
    std::istringstream f(s.substr(rp + 2));
    std::string tok;
    for (int i = 3; i <= 22 && (f >> tok); ++i) {
        if (i == 22) out.start_time = std::strtoull(tok.c_str(), nullptr, 10);
    }
    out.pid = pid;
    if (read_file(dir + "/comm", s)) {
        while (!s.empty() && (s.back() == '\n' || s.back() == '\r')) s.pop_back();
        out.comm = s;
    }
    if (read_file(dir + "/status", s)) {
        const size_t u = s.find("\nUid:");
        if (u != std::string::npos) out.uid = static_cast<uint32_t>(std::strtoul(s.c_str() + u + 5, nullptr, 10));
    }
    if (read_file(dir + "/cgroup", s)) out.unit = unit_of(s);
    return true;
}

ScanResult scan_mappers(const std::unordered_set<std::string>& files, int32_t self, const std::string& proc_root) {
    ScanResult r;
    if (files.empty()) return r;
    DIR* d = ::opendir(proc_root.c_str());
    if (d == nullptr) return r;
    while (dirent* e = ::readdir(d)) {
        char* end = nullptr;
        const long pid = std::strtol(e->d_name, &end, 10);
        if (end == e->d_name || *end != '\0' || pid <= 0 || pid == self) continue;
        ++r.processes;
        std::ifstream maps(proc_root + "/" + e->d_name + "/maps");
        if (!maps) { ++r.unreadable; continue; }
        std::vector<std::pair<std::string, bool>> found;
        for (std::string line; std::getline(maps, line);) {
            // address perms offset dev inode [pathname]: the path starts at the first '/'.
            const size_t slash = line.find('/');
            if (slash == std::string::npos) continue;
            std::string path = line.substr(slash);
            const bool deleted = path.size() > 10 && path.compare(path.size() - 10, 10, " (deleted)") == 0;
            if (deleted) path.resize(path.size() - 10);
            if (files.count(path) == 0) continue;
            bool seen = false;
            for (const auto& p : found) seen = seen || (p.first == path && p.second == deleted);
            if (!seen) found.emplace_back(path, deleted);
        }
        if (maps.bad()) { ++r.unreadable; continue; }
        if (found.empty()) continue;
        Mapper id;
        if (!read_identity(proc_root, static_cast<int32_t>(pid), id)) continue; // exited meanwhile
        for (auto& p : found) {
            Mapper m = id;
            m.path = std::move(p.first);
            m.deleted = p.second;
            r.mappers.push_back(std::move(m));
        }
    }
    ::closedir(d);
    return r;
}

} // namespace subplane::observability
