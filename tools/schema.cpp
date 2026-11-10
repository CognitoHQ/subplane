// subplane schema: schema tooling.
//
//   subplane schema --schema X.xml --views cpp|numpy|q|all --out DIR [--namespace NS] [--message M[,M...]] [--stdout]
//   subplane schema --schema X.xml --codec cpp --out DIR [--namespace NS] [--stdout]
//   subplane schema --schema X.xml --lint [--control] [--routing F,F...] [--admin ID,ID...] [--against OLD.xml] [--strict]
//
// --views writes <package>_views.h / .py / .q (package from the XML, "." -> "_"):
// typed views for flat messages; prints which were emitted and which skipped.
// --codec writes <package>_codec.h: encoders and decoders, groups and varData included.
// --lint checks subplane's schema rules (sbe_lint.h); exit 1 on errors (or warnings with --strict).
// Exit 0 on success, 1 on schema/write errors, 2 on usage.

#include "commands.h"
#include "tool_common.h"

#include "subplane/decoder/sbe_codec.h"
#include "subplane/decoder/sbe_lint.h"
#include "subplane/decoder/sbe_views.h"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <sstream>

using namespace subplane;

namespace {

void usage() {
    std::fputs("usage: subplane schema --schema X.xml --views cpp|numpy|q|all --out DIR [--namespace NS] [--message M[,M...]] [--stdout]\n"
               "       subplane schema --schema X.xml --codec cpp --out DIR [--namespace NS] [--stdout]\n"
               "       subplane schema --schema X.xml --lint [--control] [--routing F,F...] [--admin ID,ID...] [--against OLD.xml] [--strict]\n", stderr);
}

// Writes DIR/NAME, creating DIR if needed; returns the path written (empty on error).
std::string write_file(const std::string& dir, const std::string& name, const std::string& body, std::string& err) {
    const std::filesystem::path out = dir.empty() ? std::filesystem::path(".") : std::filesystem::path(dir);
    std::error_code ec;
    std::filesystem::create_directories(out, ec);
    if (ec) { err = "cannot create " + out.string() + ": " + ec.message(); return {}; }
    const std::string path = (out / name).string();
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    if (!f) { err = "cannot write " + path; return {}; }
    f << body;
    if (!f) { err = "cannot write " + path; return {}; }
    return path;
}

} // namespace

int cmd_schema(int argc, char** argv) {
    sptool::Args args;
    std::string err;
    if (!args.parse(argc, argv, {"schema", "views", "codec", "out", "namespace", "message", "routing", "admin", "against"}, {"stdout", "help", "lint", "control", "strict"}, err)) {
        std::fprintf(stderr, "subplane schema: %s\n", err.c_str()); usage(); return 2;
    }
    if (args.has("help") || !args.has("schema")) { usage(); return 2; }

    SbeSchema schema;
    if (!schema.load_file(args.str("schema", ""), err)) { std::fprintf(stderr, "subplane schema: schema: %s\n", err.c_str()); return 1; }

    if (args.has("lint")) {
        LintOptions lo;
        lo.control = args.has("control");
        {
            std::stringstream ss(args.str("routing", ""));
            std::string f;
            while (std::getline(ss, f, ',')) if (!f.empty()) lo.routing_fields.push_back(f);
        }
        {
            std::stringstream ss(args.str("admin", ""));
            std::string id;
            while (std::getline(ss, id, ',')) if (!id.empty()) lo.admin_ids.push_back(static_cast<uint16_t>(std::stoul(id)));
        }
        SbeSchema base;
        if (args.has("against")) {
            if (!base.load_file(args.str("against", ""), err)) { std::fprintf(stderr, "subplane schema: --against: %s\n", err.c_str()); return 1; }
            lo.baseline = &base;
        }
        int errors = 0, warnings = 0;
        for (const auto& f : lint_schema(schema, lo)) {
            const bool e = f.level == LintFinding::Level::ERROR;
            (e ? errors : warnings) += 1;
            std::printf("%s: %s: %s\n", e ? "error" : "warning", f.where.c_str(), f.message.c_str());
        }
        std::printf("%d error(s), %d warning(s)\n", errors, warnings);
        return errors > 0 || (args.has("strict") && warnings > 0) ? 1 : 0;
    }
    if (args.has("views") == args.has("codec") || (!args.has("out") && !args.has("stdout"))) { usage(); return 2; }

    if (args.has("codec")) {
        if (args.str("codec", "") != "cpp") { std::fprintf(stderr, "subplane schema: --codec accepts cpp\n"); return 2; }
        CodecOptions copt;
        copt.ns = args.str("namespace", "sp_codec");
        std::string body;
        if (!emit_cpp_codec(schema, copt, body, err)) { std::fprintf(stderr, "subplane schema: codec: %s\n", err.c_str()); return 1; }
        if (args.has("stdout")) { std::fwrite(body.data(), 1, body.size(), stdout); return 0; }
        const std::string path = write_file(args.str("out", ""), views_basename(schema) + "_codec.h", body, err);
        if (path.empty()) { std::fprintf(stderr, "subplane schema: %s\n", err.c_str()); return 1; }
        std::fprintf(stderr, "wrote %s\n", path.c_str());
        return 0;
    }

    ViewsOptions opt;
    opt.ns = args.str("namespace", "sp_views");
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
                if (k != "cpp" && k != "numpy" && k != "q") { std::fprintf(stderr, "subplane schema: --views accepts cpp, numpy, q or all\n"); return 2; }
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
        if (!ok) { std::fprintf(stderr, "subplane schema: %s: %s\n", k.c_str(), err.c_str()); return 1; }
        rep = r;
        if (args.has("stdout")) {
            std::fwrite(body.data(), 1, body.size(), stdout);
        } else {
            const std::string path = write_file(dir, base + "_views." + (k == "cpp" ? "h" : k == "numpy" ? "py" : "q"), body, err);
            if (path.empty()) { std::fprintf(stderr, "subplane schema: %s\n", err.c_str()); return 1; }
            std::fprintf(stderr, "wrote %s\n", path.c_str());
        }
    }
    for (const auto& m : rep.emitted) std::fprintf(stderr, "emitted %s\n", m.c_str());
    for (const auto& m : rep.skipped) std::fprintf(stderr, "skipped %s\n", m.c_str());
    return 0;
}
