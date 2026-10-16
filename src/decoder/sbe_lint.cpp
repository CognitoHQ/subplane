#include "subplane/decoder/sbe_lint.h"

#include <algorithm>
#include <map>

namespace subplane {
namespace {

using Level = LintFinding::Level;

struct Out {
    std::vector<LintFinding> v;
    void err(const std::string& w, const std::string& m) { v.push_back({Level::ERROR, w, m}); }
    void warn(const std::string& w, const std::string& m) { v.push_back({Level::WARNING, w, m}); }
};

uint16_t scalar_size(SbePrim p) {
    switch (p) {
        case SbePrim::U8: case SbePrim::I8: return 1;
        case SbePrim::U16: case SbePrim::I16: return 2;
        case SbePrim::U32: case SbePrim::I32: case SbePrim::F32: return 4;
        case SbePrim::U64: case SbePrim::I64: case SbePrim::F64: return 8;
        default: return 1;
    }
}

const FieldDef* field(const std::vector<FieldDef>& fs, const std::string& n) {
    for (const auto& f : fs) if (f.name == n) return &f;
    return nullptr;
}

std::string key(const FieldDef& f) { return f.type_name + "/" + std::to_string(f.size); }

void check_fields_evolution(Out& out, const std::string& where, const std::vector<FieldDef>& oldf, const std::vector<FieldDef>& newf,
                            uint16_t old_block, uint16_t old_version, uint16_t new_version) {
    // INTENT: constants (presence="constant") occupy no bytes, so their offset
    // shifts whenever a field is appended before them; that is not a move.
    for (const auto& of : oldf) {
        if (of.size == 0) continue;
        const FieldDef* nf = field(newf, of.name);
        if (!nf) { out.err(where + "." + of.name, "removed; fields are only ever appended (a removal needs a new schemaId)"); continue; }
        if (nf->offset != of.offset) out.err(where + "." + of.name, "moved from offset " + std::to_string(of.offset) + " to " + std::to_string(nf->offset));
        if (key(*nf) != key(of)) out.err(where + "." + of.name, "type changed from " + of.type_name + " to " + nf->type_name);
    }
    for (const auto& nf : newf) {
        if (nf.size == 0 || field(oldf, nf.name)) continue;
        if (nf.offset < old_block) out.err(where + "." + nf.name, "added inside the old block (offset " + std::to_string(nf.offset) + " < " + std::to_string(old_block) + "); append after it");
        if (nf.since_version == 0 || nf.since_version <= old_version)
            out.err(where + "." + nf.name, "added without sinceVersion=" + std::to_string(new_version));
    }
}

void check_evolution(Out& out, const SbeSchema& s, const SbeSchema& b) {
    if (s.schema_id() != b.schema_id()) {
        out.err("schema", "schemaId changed from " + std::to_string(b.schema_id()) + " to " + std::to_string(s.schema_id()) + "; a new id is a new schema, not an evolution");
        return;
    }
    if (s.version() < b.version()) out.err("schema", "version went down");
    bool changed = false;
    for (const auto& om : b.messages()) {
        const MessageDef* nm = s.find_message(om.template_id);
        const std::string w = om.name;
        if (!nm) { out.err(w, "removed (template " + std::to_string(om.template_id) + ")"); continue; }
        if (nm->name != om.name) out.warn(w, "renamed to " + nm->name + "; generated code changes name");
        if (nm->block_length < om.block_length) out.err(w, "block shrank from " + std::to_string(om.block_length) + " to " + std::to_string(nm->block_length));
        if (nm->block_length != om.block_length || nm->fields.size() != om.fields.size() || nm->groups.size() != om.groups.size() || nm->data.size() != om.data.size())
            changed = true;
        check_fields_evolution(out, w, om.fields, nm->fields, om.block_length, b.version(), s.version());
        for (size_t i = 0; i < om.groups.size(); ++i) {
            if (i >= nm->groups.size() || nm->groups[i].name != om.groups[i].name) { out.err(w + "." + om.groups[i].name, "group removed or reordered"); continue; }
            check_fields_evolution(out, w + "." + om.groups[i].name, om.groups[i].fields, nm->groups[i].fields, om.groups[i].block_length, b.version(), s.version());
        }
        for (size_t i = om.groups.size(); i < nm->groups.size(); ++i)
            if (nm->groups[i].since_version <= b.version()) out.err(w + "." + nm->groups[i].name, "group added without sinceVersion=" + std::to_string(s.version()));
        if (!om.data.empty() && nm->groups.size() > om.groups.size())
            out.err(w, "group added after existing varData; groups must precede data, so this cannot be appended");
        for (size_t i = 0; i < om.data.size(); ++i)
            if (i >= nm->data.size() || nm->data[i].name != om.data[i].name) out.err(w + "." + om.data[i].name, "data removed or reordered");
        for (size_t i = om.data.size(); i < nm->data.size(); ++i)
            if (nm->data[i].since_version <= b.version()) out.err(w + "." + nm->data[i].name, "data added without sinceVersion=" + std::to_string(s.version()));
    }
    for (const auto& nm : s.messages()) if (!b.find_message(nm.template_id)) changed = true;
    if (changed && s.version() == b.version()) out.err("schema", "messages changed but the version did not; bump it to " + std::to_string(b.version() + 1));
}

} // namespace

std::vector<LintFinding> lint_schema(const SbeSchema& s, const LintOptions& o) {
    Out out;
    if (!s.little_endian()) out.warn("schema", "big-endian: the agent and the generated views and codecs are little-endian");
    const HeaderLayout& h = s.header();
    if (h.size != 8 || h.block_length_size != 2 || h.template_id_size != 2 || h.schema_id_size != 2 || h.version_size != 2)
        out.warn("schema", "non-standard message header; the codec generator needs the 8-byte SBE header");

    std::map<std::string, std::map<uint16_t, std::vector<std::string>>> routing_offsets; // field -> offset -> messages
    for (const auto& m : s.messages()) {
        const bool admin = std::find(o.admin_ids.begin(), o.admin_ids.end(), m.template_id) != o.admin_ids.end();
        if (o.control) {
            const FieldDef* c = m.fields.empty() ? nullptr : &m.fields.front();
            if (!c || c->name != "CorrelationId" || c->offset != 0 || c->prim != SbePrim::U64)
                out.err(m.name, "a control message's root must start with CorrelationId (uint64, offset 0)");
            if (m.template_id >= 900 && m.template_id != 900 && m.template_id < 1000)
                out.warn(m.name, "template ids 901-999 are reserved; core messages use 1-899");
        }
        for (const auto& f : m.fields) {
            if (f.size == 0) continue;
            const uint16_t a = f.prim == SbePrim::OPAQUE || f.prim == SbePrim::CHAR_ARRAY ? scalar_size(f.elem_prim) : scalar_size(f.prim);
            if ((h.size + f.offset) % a != 0)
                out.warn(m.name + "." + f.name, "not naturally aligned at payload offset " + std::to_string(h.size + f.offset) +
                         "; typed views fall back to accessors");
            if (!o.control && !admin && (f.prim == SbePrim::F32 || f.prim == SbePrim::F64))
                out.warn(m.name + "." + f.name, "floating point; subplane uses int64 fixed point (the canonical schema scales by 1e-9)");
        }
        if (!o.control && !admin) {
            if (!m.groups.empty() || !m.data.empty())
                out.warn(m.name, "groups or varData in a data message: no typed views, and the agent must walk it to find the next message; prefer flat");
            for (const auto& r : o.routing_fields) {
                const FieldDef* f = field(m.fields, r);
                if (!f) { out.err(m.name, "routing field " + r + " missing from the root block"); continue; }
                routing_offsets[r][f->offset].push_back(m.name);
                if (f->prim == SbePrim::CHAR_ARRAY && (r.find("Instrument") != std::string::npos || r.find("Symbol") != std::string::npos))
                    out.warn(m.name + "." + r, "a string instrument; integer ids route faster, with names in a definition message");
            }
        }
    }
    for (const auto& [r, offs] : routing_offsets) {
        if (offs.size() <= 1) continue;
        std::string detail;
        for (const auto& [off, ms] : offs) detail += " " + std::to_string(off) + ":" + ms.front() + (ms.size() > 1 ? "+" + std::to_string(ms.size() - 1) : "");
        out.warn(r, "routing field at different offsets across messages (" + detail.substr(1) + "); one shared prefix keeps routing a fixed read");
    }
    if (o.baseline) check_evolution(out, s, *o.baseline);
    return out.v;
}

} // namespace subplane
