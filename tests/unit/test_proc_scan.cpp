#include <doctest.h>

#include "subplane/observability/proc_scan.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <thread>

using namespace subplane::observability;

namespace {
void write(const std::string& path, const std::string& body) {
    std::ofstream f(path);
    f << body;
}
} // namespace

TEST_CASE("proc scan: a fake /proc: paths matched exactly, comm with ')' and spaces, cgroup unit, unreadable maps") {
    char tmpl[] = "/tmp/subplane-procXXXXXX";
    REQUIRE(::mkdtemp(tmpl) != nullptr);
    const std::string root = tmpl;
    auto proc = [&](int pid, const std::string& maps, const std::string& comm, const std::string& cgroup) {
        const std::string d = root + "/" + std::to_string(pid);
        ::mkdir(d.c_str(), 0755);
        if (!maps.empty()) write(d + "/maps", maps);
        // stat: pid (comm) state ppid ... field 22 = starttime.
        std::string stat = std::to_string(pid) + " (" + comm + ") S";
        for (int i = 4; i <= 21; ++i) stat += " " + std::to_string(i);
        stat += " 777" + std::to_string(pid) + " 23 24\n";
        write(d + "/stat", stat);
        write(d + "/comm", comm + "\n");
        write(d + "/status", "Name:\t" + comm + "\nUid:\t1000\t1000\t1000\t1000\n");
        write(d + "/cgroup", cgroup);
    };
    const std::string ring = "/dev/shm/subplane.a.mm.ring";
    proc(10, "7f00-7f10 r--s 00000000 00:1a 99 " + ring + "\n7f10-7f20 r--s 00000000 00:1a 99 " + ring + "\n",
         "mm engine (v2)", "0::/system.slice/mm-engine.service\n");
    proc(11, "7f00-7f10 r--s 00000000 00:1a 98 /dev/shm/subplane.a.other.ring\n", "x", "0::/\n");
    proc(12, "7f00-7f10 r--s 00000000 00:1a 99 " + ring + " (deleted)\n", "old", "0::/user.slice/session-3.scope\n");
    proc(13, "", "nomaps", "");           // no maps file: unreadable
    ::mkdir((root + "/self").c_str(), 0755); // not a pid
    proc(14, "7f00-7f10 r--s 00000000 00:1a 99 " + ring + "\n", "me", "");

    const ScanResult r = scan_mappers({ring}, 14, root);
    CHECK(r.processes == 4); // 10, 11, 12, 13; 14 is self
    CHECK(r.unreadable == 1);
    REQUIRE(r.mappers.size() == 2);
    const Mapper& a = r.mappers[0].pid == 10 ? r.mappers[0] : r.mappers[1];
    const Mapper& b = r.mappers[0].pid == 10 ? r.mappers[1] : r.mappers[0];
    CHECK(a.pid == 10);
    CHECK(a.comm == "mm engine (v2)");
    CHECK(a.start_time == 77710);
    CHECK(a.uid == 1000);
    CHECK(a.unit == "mm-engine.service");
    CHECK(a.path == ring);
    CHECK_FALSE(a.deleted);
    CHECK(b.pid == 12);   // still mapping a ring an agent restart unlinked
    CHECK(b.deleted);
    CHECK(b.unit.empty()); // login sessions are not units
    CHECK(std::system(("rm -rf " + root).c_str()) == 0);
}

TEST_CASE("proc scan: the real /proc finds a child process that maps a file") {
    char path[] = "/dev/shm/subplane-scantestXXXXXX";
    const int fd = ::mkstemp(path);
    REQUIRE(fd >= 0);
    REQUIRE(::ftruncate(fd, 4096) == 0);
    int pipefd[2];
    REQUIRE(::pipe(pipefd) == 0);
    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
        void* p = ::mmap(nullptr, 4096, PROT_READ, MAP_SHARED, fd, 0);
        const char ok = p == MAP_FAILED ? 'n' : 'y';
        [[maybe_unused]] ssize_t w = ::write(pipefd[1], &ok, 1);
        ::sleep(10);
        ::_exit(0);
    }
    char ok = 0;
    REQUIRE(::read(pipefd[0], &ok, 1) == 1);
    REQUIRE(ok == 'y');
    const ScanResult r = scan_mappers({path}, static_cast<int32_t>(::getpid()));
    bool found = false;
    for (const auto& m : r.mappers) found = found || (m.pid == child && m.path == path && m.uid == ::getuid());
    CHECK(found);
    ::kill(child, SIGKILL);
    ::waitpid(child, nullptr, 0);
    ::close(fd);
    ::unlink(path);
}
