#include "flashtick/decoder/sbe_schema.h"

#include <cstdlib>
#include <fstream>
#include <map>
#include <sstream>

#include "pugixml.hpp"

namespace flashtick {
namespace {

struct Member {
    std::string name;
    uint16_t    offset = 0;
    uint16_t    size   = 0;
    SbePrim     prim   = SbePrim::U8;
    uint16_t    length = 1;
};

struct TypeInfo {
    SbePrim  prim      = SbePrim::U8;
    uint16_t size      = 0;
    uint16_t length    = 1;
    bool     composite = false;
    std::vector<Member> members;
};

std::string_view local_name(const char* n) {
    std::string_view s(n);
    auto p = s.find(':');
    return p == std::string_view::npos ? s : s.substr(p + 1);
}

bool prim_from_name(std::string_view n, SbePrim& p, uint16_t& sz) {
    struct E { const char* n; SbePrim p; uint16_t s; };
    static const E tbl[] = {
        {"char", SbePrim::CHAR_ARRAY, 1}, {"int8", SbePrim::I8, 1}, {"uint8", SbePrim::U8, 1},
        {"int16", SbePrim::I16, 2}, {"uint16", SbePrim::U16, 2}, {"int32", SbePrim::I32, 4},
        {"uint32", SbePrim::U32, 4}, {"int64", SbePrim::I64, 8}, {"uint64", SbePrim::U64, 8},
        {"float", SbePrim::F32, 4}, {"double", SbePrim::F64, 8}};
    for (const auto& e : tbl)
        if (n == e.n) { p = e.p; sz = e.s; return true; }
    return false;
}

bool parse_uint(const char* s, unsigned long max, unsigned long& out) {
    if (!s || !*s) return false;
    char* end = nullptr;
    unsigned long v = std::strtoul(s, &end, 0);
    if (*end != '\0' || v > max) return false;
    out = v;
    return true;
}

struct Loader {
    std::map<std::string, TypeInfo, std::less<>> types;
    std::string err;

    bool fail(const std::string& m) { err = m; return false; }

    // Resolve a type name to layout: builtin primitive or a declared type.
    bool resolve(std::string_view name, TypeInfo& out) {
        SbePrim p; uint16_t sz;
        if (prim_from_name(name, p, sz)) {
            out = TypeInfo{p, sz, 1, false, {}};
            return true;
        }
        auto it = types.find(name);
        if (it == types.end()) return fail("unknown type '" + std::string(name) + "'");
        out = it->second;
        return true;
    }

    // <type name primitiveType length? presence?> -> TypeInfo
    bool parse_simple_type(const pugi::xml_node& n, TypeInfo& t) {
        const char* pt = n.attribute("primitiveType").as_string();
        SbePrim p; uint16_t sz;
        if (!prim_from_name(pt, p, sz))
            return fail("unknown primitive type '" + std::string(pt) + "' in type '" +
                        n.attribute("name").as_string() + "'");
        unsigned long len = 1;
        if (n.attribute("length") && !parse_uint(n.attribute("length").value(), 65535, len))
            return fail(std::string("bad length in type '") + n.attribute("name").as_string() + "'");
        t.prim = p; t.length = static_cast<uint16_t>(len);
        unsigned long total = static_cast<unsigned long>(sz) * len;
        if (total > 65535) return fail("type too large: " + std::string(n.attribute("name").as_string()));
        t.size = static_cast<uint16_t>(total);
        if (len != 1 && p != SbePrim::CHAR_ARRAY) t.prim = SbePrim::OPAQUE;
        if (std::string_view(n.attribute("presence").as_string()) == "constant") t.size = 0;
        return true;
    }

    bool parse_composite(const pugi::xml_node& n, TypeInfo& t) {
        t.composite = true;
        t.prim = SbePrim::OPAQUE;
        uint32_t cursor = 0;
        for (auto m : n.children()) {
            if (m.type() != pugi::node_element) continue;
            std::string_view ln = local_name(m.name());
            Member mem;
            mem.name = m.attribute("name").as_string();
            TypeInfo mt;
            if (ln == "type") {
                if (!parse_simple_type(m, mt)) return false;
            } else if (ln == "ref") {
                if (!resolve(m.attribute("type").as_string(), mt)) return false;
            } else if (ln == "enum" || ln == "set") {
                if (!resolve(m.attribute("encodingType").as_string(), mt)) return false;
            } else {
                return fail("unsupported composite member <" + std::string(ln) + "> in '" +
                            n.attribute("name").as_string() + "'");
            }
            unsigned long off = cursor;
            if (m.attribute("offset") && !parse_uint(m.attribute("offset").value(), 65535, off))
                return fail("bad offset in composite '" + std::string(n.attribute("name").as_string()) + "'");
            mem.offset = static_cast<uint16_t>(off);
            mem.size = mt.size; mem.prim = mt.prim; mem.length = mt.length;
            cursor = static_cast<uint32_t>(off) + mt.size;
            if (cursor > 65535) return fail("composite too large");
            t.members.push_back(std::move(mem));
        }
        t.size = static_cast<uint16_t>(cursor);
        return true;
    }

    bool parse_types(const pugi::xml_node& types_node) {
        for (auto n : types_node.children()) {
            if (n.type() != pugi::node_element) continue;
            std::string_view ln = local_name(n.name());
            std::string name = n.attribute("name").as_string();
            if (name.empty()) return fail("<" + std::string(ln) + "> without name in <types>");
            TypeInfo t;
            if (ln == "type") {
                if (!parse_simple_type(n, t)) return false;
            } else if (ln == "composite") {
                if (!parse_composite(n, t)) return false;
            } else if (ln == "enum" || ln == "set") {
                if (!resolve(n.attribute("encodingType").as_string(), t)) return false;
            } else {
                continue; // tolerate unknown elements (e.g. documentation)
            }
            types[name] = std::move(t);
        }
        return true;
    }

    const Member* find_member(const TypeInfo& c, std::string_view n) {
        for (const auto& m : c.members) if (m.name == n) return &m;
        return nullptr;
    }

    bool parse_header(const std::string& header_type, HeaderLayout& h) {
        auto it = types.find(header_type);
        if (it == types.end() || !it->second.composite)
            return fail("headerType '" + header_type + "' is not a composite");
        const TypeInfo& c = it->second;
        struct R { const char* n; uint16_t* off; uint8_t* sz; };
        R reqs[] = {{"blockLength", &h.block_length_off, &h.block_length_size},
                    {"templateId", &h.template_id_off, &h.template_id_size},
                    {"schemaId", &h.schema_id_off, &h.schema_id_size},
                    {"version", &h.version_off, &h.version_size}};
        for (auto& r : reqs) {
            const Member* m = find_member(c, r.n);
            if (!m) return fail("header composite '" + header_type + "' lacks member " + r.n);
            if (m->size != 1 && m->size != 2 && m->size != 4)
                return fail(std::string("header member ") + r.n + " must be 1, 2 or 4 bytes");
            *r.off = m->offset; *r.sz = static_cast<uint8_t>(m->size);
        }
        h.size = c.size;
        return true;
    }

    bool parse_dimension(const std::string& type, DimensionDef& d) {
        auto it = types.find(type);
        if (it == types.end() || !it->second.composite)
            return fail("dimensionType '" + type + "' is not a composite");
        const Member* b = find_member(it->second, "blockLength");
        const Member* n = find_member(it->second, "numInGroup");
        if (!b || !n) return fail("dimension composite '" + type + "' needs blockLength and numInGroup");
        if ((b->size != 1 && b->size != 2 && b->size != 4) || (n->size != 1 && n->size != 2 && n->size != 4))
            return fail("dimension members of '" + type + "' must be 1, 2 or 4 bytes");
        d.size = it->second.size;
        d.block_length_off = b->offset; d.block_length_size = static_cast<uint8_t>(b->size);
        d.num_in_group_off = n->offset; d.num_in_group_size = static_cast<uint8_t>(n->size);
        return true;
    }

    bool parse_var_data(const pugi::xml_node& n, VarDataDef& v) {
        v.name = n.attribute("name").as_string();
        unsigned long id = 0;
        if (!parse_uint(n.attribute("id").value(), 65535, id)) return fail("data '" + v.name + "' needs a valid id");
        v.id = static_cast<uint16_t>(id);
        std::string type = n.attribute("type").as_string();
        auto it = types.find(type);
        if (it == types.end() || !it->second.composite) return fail("data '" + v.name + "': type '" + type + "' is not a composite");
        const Member* l = find_member(it->second, "length");
        if (!l) return fail("varData composite '" + type + "' lacks 'length' member");
        if (l->size != 1 && l->size != 2 && l->size != 4) return fail("varData length of '" + type + "' must be 1, 2 or 4 bytes");
        v.len_off = l->offset; v.len_size = static_cast<uint8_t>(l->size);
        v.hdr_size = it->second.size;
        unsigned long sv = 0;
        if (n.attribute("sinceVersion") && !parse_uint(n.attribute("sinceVersion").value(), 65535, sv))
            return fail("data '" + v.name + "' has bad sinceVersion");
        v.since_version = static_cast<uint16_t>(sv);
        return true;
    }

    // Fields, groups, data of a message or group body.
    template <class Def>
    bool parse_body(const pugi::xml_node& node, Def& def, uint32_t& cursor) {
        for (auto c : node.children()) {
            if (c.type() != pugi::node_element) continue;
            std::string_view ln = local_name(c.name());
            if (ln == "field") {
                FieldDef f;
                f.name = c.attribute("name").as_string();
                unsigned long id = 0;
                if (!parse_uint(c.attribute("id").value(), 65535, id)) return fail("field '" + f.name + "' needs a valid id");
                f.id = static_cast<uint16_t>(id);
                TypeInfo t;
                std::string type = c.attribute("type").as_string();
                if (!resolve(type, t)) return fail("field '" + f.name + "': " + err);
                f.prim = t.prim; f.size = t.size; f.length = t.length;
                if (std::string_view(c.attribute("presence").as_string()) == "constant") f.size = 0;
                unsigned long off = cursor;
                if (c.attribute("offset") && !parse_uint(c.attribute("offset").value(), 65535, off))
                    return fail("field '" + f.name + "' has bad offset");
                if (off + f.size > 65535) return fail("field '" + f.name + "' exceeds 64KiB block");
                f.offset = static_cast<uint16_t>(off);
                cursor = static_cast<uint32_t>(off) + f.size;
                def.fields.push_back(std::move(f));
            } else if (ln == "group") {
                GroupDef g;
                g.name = c.attribute("name").as_string();
                unsigned long id = 0;
                if (!parse_uint(c.attribute("id").value(), 65535, id)) return fail("group '" + g.name + "' needs a valid id");
                g.id = static_cast<uint16_t>(id);
                if (!parse_dimension(c.attribute("dimensionType").as_string("groupSizeEncoding"), g.dim)) return false;
                uint32_t gcur = 0;
                if (!parse_group_body(c, g, gcur)) return false;
                unsigned long bl = gcur;
                if (c.attribute("blockLength") && !parse_uint(c.attribute("blockLength").value(), 65535, bl))
                    return fail("group '" + g.name + "' has bad blockLength");
                g.block_length = static_cast<uint16_t>(bl);
                unsigned long sv = 0;
                if (c.attribute("sinceVersion") && !parse_uint(c.attribute("sinceVersion").value(), 65535, sv))
                    return fail("group '" + g.name + "' has bad sinceVersion");
                g.since_version = static_cast<uint16_t>(sv);
                def.groups.push_back(std::move(g));
            } else if (ln == "data") {
                VarDataDef v;
                if (!parse_var_data(c, v)) return false;
                def.data.push_back(std::move(v));
            }
        }
        return true;
    }

    bool parse_group_body(const pugi::xml_node& node, GroupDef& g, uint32_t& cursor) {
        return parse_body(node, g, cursor);
    }
};

bool has_name(const std::vector<FieldDef>& f, std::string_view n) {
    for (const auto& x : f) if (x.name == n) return true;
    return false;
}
bool group_has(const GroupDef& g, std::string_view n) {
    if (g.name == n || has_name(g.fields, n)) return true;
    for (const auto& d : g.data) if (d.name == n) return true;
    for (const auto& s : g.groups) if (group_has(s, n)) return true;
    return false;
}

} // namespace

bool SbeSchema::load_file(const std::string& path, std::string& err) {
    std::ifstream in(path, std::ios::binary);
    if (!in) { err = "cannot open schema file '" + path + "'"; return false; }
    std::ostringstream ss;
    ss << in.rdbuf();
    return load_string(ss.str(), err);
}

bool SbeSchema::load_string(std::string_view xml, std::string& err) {
    messages_.clear();
    pugi::xml_document doc;
    pugi::xml_parse_result r = doc.load_buffer(xml.data(), xml.size());
    if (!r) {
        err = std::string("XML parse error: ") + r.description() + " at offset " + std::to_string(r.offset);
        return false;
    }
    pugi::xml_node root;
    for (auto n : doc.children())
        if (n.type() == pugi::node_element && local_name(n.name()) == "messageSchema") { root = n; break; }
    if (!root) { err = "no <messageSchema> root element"; return false; }

    unsigned long v = 0;
    if (!parse_uint(root.attribute("id").value(), 65535, v)) { err = "messageSchema needs a valid id"; return false; }
    schema_id_ = static_cast<uint16_t>(v);
    v = 0;
    if (!parse_uint(root.attribute("version").value(), 65535, v)) { err = "messageSchema needs a valid version"; return false; }
    version_ = static_cast<uint16_t>(v);
    std::string_view bo = root.attribute("byteOrder").as_string("littleEndian");
    if (bo == "littleEndian") little_endian_ = true;
    else if (bo == "bigEndian") little_endian_ = false;
    else { err = "bad byteOrder '" + std::string(bo) + "'"; return false; }

    Loader L;
    for (auto n : root.children())
        if (n.type() == pugi::node_element && local_name(n.name()) == "types")
            if (!L.parse_types(n)) { err = L.err; return false; }

    header_ = HeaderLayout{};
    if (!L.parse_header(root.attribute("headerType").as_string("messageHeader"), header_)) { err = L.err; return false; }

    for (auto n : root.children()) {
        if (n.type() != pugi::node_element || local_name(n.name()) != "message") continue;
        MessageDef m;
        m.name = n.attribute("name").as_string();
        unsigned long id = 0;
        if (!parse_uint(n.attribute("id").value(), 65535, id)) { err = "message '" + m.name + "' needs a valid id"; return false; }
        m.template_id = static_cast<uint16_t>(id);
        for (const auto& e : messages_)
            if (e.template_id == m.template_id) { err = "duplicate message id " + std::to_string(id); return false; }
        uint32_t cursor = 0;
        if (!L.parse_body(n, m, cursor)) { err = "message '" + m.name + "': " + L.err; return false; }
        unsigned long bl = cursor;
        if (n.attribute("blockLength") && !parse_uint(n.attribute("blockLength").value(), 65535, bl)) {
            err = "message '" + m.name + "' has bad blockLength"; return false;
        }
        m.block_length = static_cast<uint16_t>(bl);
        messages_.push_back(std::move(m));
    }
    if (messages_.empty()) { err = "schema defines no messages"; return false; }
    return true;
}

const MessageDef* SbeSchema::find_message(uint16_t template_id) const {
    for (const auto& m : messages_) if (m.template_id == template_id) return &m;
    return nullptr;
}

const FieldDef* SbeSchema::find_field(const MessageDef& m, std::string_view name) const {
    for (const auto& f : m.fields) if (f.name == name) return &f;
    return nullptr;
}

bool SbeSchema::is_nested_field(const MessageDef& m, std::string_view name) const {
    if (find_field(m, name)) return false;
    for (const auto& d : m.data) if (d.name == name) return true;
    for (const auto& g : m.groups) if (group_has(g, name)) return true;
    return false;
}

} // namespace flashtick
