// ft_schema: schema tooling. Today: typed views for flat SBE messages.
//
//   ft_schema --schema X.xml --views cpp|numpy|q|all --out DIR [--namespace NS] [--message M[,M...]] [--stdout]
//
// Writes <package>_views.h / .py / .q into DIR (package from the XML, "." -> "_").
// Prints which messages were emitted and which were skipped (groups, varData).
// Exit 0 when at least one message was emitted, 1 on schema/write errors, 2 on usage.

#include "tool_common.h"

#include "flashtick/decoder/sbe_views.h"

#include <cstdio>
#include <fstream>
#include <sstream>

using namespace flashtick;

namespace {

void usage() {
    std::fputs("usage: ft_schema --schema X.xml --views cpp|numpy|q|all --out DIR [--namespace NS] [--message M[,M...]] [--stdout]\n", stderr);
}

bool write_file(const std::string& path, const std::string& body, std::string& err) {
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    if (!f) { err = "cannot write " + path; return false; }
    f << body;
    return static_cast<bool>(f);
}

} // namespace

int main(int argc, char** argv) {
    fttool::Args args;
    std::string err;
    if (!args.parse(argc, argv, {"schema", "views", "out", "namespace", "message"}, {"stdout", "help"}, err)) {
        std::fprintf(stderr, "ft_schema: %s\n", err.c_str()); usage(); return 2;
    }
    if (args.has("help") || !args.has("schema") || !args.has("views") || (!args.has("out") && !args.has("stdout"))) { usage(); return 2; }

    SbeSchema schema;
    if (!schema.load_file(args.str("schema", ""), err)) { std::fprintf(stderr, "ft_schema: schema: %s\n", err.c_str()); return 1; }

    ViewsOptions opt;
    opt.ns = args.str("namespace", "ft_views");
    {
        std::stringstream ss(args.str("message", ""));
        std::string m;
        while (std::getline(ss, m, ',')) if (!m.empty()) opt.only.push_back(m);
    }
    std::vector<std::string> kinds;
    {
        const std::string v = args.str("views", "");
        if (v == "all") kinds = {"cpp", "numpy", "q"};
        else {
            std::stringstream ss(v);
            std::string k;
            while (std::getline(ss, k, ',')) {
                if (k != "cpp" && k != "numpy" && k != "q") { std::fprintf(stderr, "ft_schema: --views accepts cpp, numpy, q or all\n"); return 2; }
                kinds.push_back(k);
            }
        }
    }
    const std::string base = views_basename(schema);
    const std::string dir = args.str("out", "");
    ViewsReport rep;
    for (const std::string& k : kinds) {
        std::string body;
        ViewsReport r;
        const bool ok = k == "cpp" ? emit_cpp(schema, opt, body, r, err) : k == "numpy" ? emit_numpy(schema, opt, body, r, err) : emit_q(schema, opt, body, r, err);
        if (!ok) { std::fprintf(stderr, "ft_schema: %s: %s\n", k.c_str(), err.c_str()); return 1; }
        rep = r;
        if (args.has("stdout")) {
            std::fwrite(body.data(), 1, body.size(), stdout);
        } else {
            const std::string path = dir + "/" + base + "_views." + (k == "cpp" ? "h" : k == "numpy" ? "py" : "q");
            if (!write_file(path, body, err)) { std::fprintf(stderr, "ft_schema: %s\n", err.c_str()); return 1; }
            std::fprintf(stderr, "wrote %s\n", path.c_str());
        }
    }
    for (const auto& m : rep.emitted) std::fprintf(stderr, "emitted %s\n", m.c_str());
    for (const auto& m : rep.skipped) std::fprintf(stderr, "skipped %s\n", m.c_str());
    return 0;
}
