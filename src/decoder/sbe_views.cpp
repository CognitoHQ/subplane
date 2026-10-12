#include "flashtick/decoder/sbe_views.h"

#include <algorithm>
#include <cctype>

namespace flashtick {
namespace {

using Kind = ViewSlot::Kind;

bool wanted(const ViewsOptions& o, const std::string& name) {
    return o.only.empty() || std::find(o.only.begin(), o.only.end(), name) != o.only.end();
}

// Selected layouts in schema order; refuses big-endian schemas.
bool collect(const SbeSchema& s, const ViewsOptions& o, std::vector<FlatLayout>& out, ViewsReport& rep, std::string& err) {
    if (!s.little_endian()) { err = "big-endian schema: typed views are little-endian only"; return false; }
    for (const std::string& n : o.only) {
        bool found = false;
        for (const auto& m : s.messages()) found = found || m.name == n;
        if (!found) { err = "no message named '" + n + "' in the schema"; return false; }
    }
    for (const auto& m : s.messages()) {
        if (!wanted(o, m.name)) continue;
        FlatLayout l;
        std::string reason;
        if (!flat_layout(s, m, l, reason)) { rep.skipped.push_back(m.name + ": " + reason); continue; }
        rep.emitted.push_back(m.name);
        out.push_back(std::move(l));
    }
    if (out.empty()) { err = "no flat message to emit"; return false; }
    return true;
}

const char* cpp_scalar(SbePrim p) {
    switch (p) {
        case SbePrim::U8: return "uint8_t";   case SbePrim::I8: return "int8_t";
        case SbePrim::U16: return "uint16_t"; case SbePrim::I16: return "int16_t";
        case SbePrim::U32: return "uint32_t"; case SbePrim::I32: return "int32_t";
        case SbePrim::U64: return "uint64_t"; case SbePrim::I64: return "int64_t";
        case SbePrim::F32: return "float";    case SbePrim::F64: return "double";
        default: return nullptr; // CHAR_ARRAY / OPAQUE are arrays
    }
}
bool scalar_array(const ViewSlot& v) { return v.prim == SbePrim::OPAQUE && v.length > 1 && cpp_scalar(v.elem) != nullptr; }
std::string np_elem(SbePrim p) {
    switch (p) {
        case SbePrim::U8: return "u1";   case SbePrim::I8: return "i1";
        case SbePrim::U16: return "<u2"; case SbePrim::I16: return "<i2";
        case SbePrim::U32: return "<u4"; case SbePrim::I32: return "<i4";
        case SbePrim::U64: return "<u8"; case SbePrim::I64: return "<i8";
        case SbePrim::F32: return "<f4"; default: return "<f8";
    }
}
// Quoted dtype format for one slot; a scalar array is an unquoted (fmt, (n,)) tuple.
std::string np_fmt(const ViewSlot& v) {
    if (scalar_array(v)) return "('" + np_elem(v.elem) + "', (" + std::to_string(v.length) + ",))";
    switch (v.prim) {
        case SbePrim::U8: return "u1";   case SbePrim::I8: return "i1";
        case SbePrim::U16: return "<u2"; case SbePrim::I16: return "<i2";
        case SbePrim::U32: return "<u4"; case SbePrim::I32: return "<i4";
        case SbePrim::U64: return "<u8"; case SbePrim::I64: return "<i8";
        case SbePrim::F32: return "<f4"; case SbePrim::F64: return "<f8";
        case SbePrim::CHAR_ARRAY: return "S" + std::to_string(v.size);
        default: return "V" + std::to_string(v.size);
    }
}
// q `1:` type char; ' ' = skip. Unsigned types are read as the signed type of the same width.
char q_type(SbePrim p) {
    switch (p) {
        case SbePrim::U8: case SbePrim::I8: return 'x';
        case SbePrim::U16: case SbePrim::I16: return 'h';
        case SbePrim::U32: case SbePrim::I32: return 'i';
        case SbePrim::U64: case SbePrim::I64: return 'j';
        case SbePrim::F32: return 'e'; case SbePrim::F64: return 'f';
        default: return ' ';
    }
}
std::string type_note(const ViewSlot& v) {
    const char* p = cpp_scalar(v.prim);
    if (v.kind != Kind::FIELD || v.type_name.empty() || (p && v.type_name == std::string(p).substr(0, 0))) return "";
    // Primitive names in SBE XML are "uint64" etc.; only note non-primitive type names.
    static const char* prims[] = {"char", "int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64", "float", "double"};
    for (const char* q : prims) if (v.type_name == q) return "";
    return v.type_name;
}
bool valid_ident(const std::string& n) {
    if (n.empty() || std::isdigit(static_cast<unsigned char>(n[0]))) return false;
    for (char c : n) if (!std::isalnum(static_cast<unsigned char>(c)) && c != '_') return false;
    return true;
}

void add_pad(FlatLayout& l, uint32_t from, uint32_t to, int& n) {
    if (to <= from) return;
    ViewSlot p; p.kind = Kind::PAD; p.name = "_pad" + std::to_string(n++); p.offset = from; p.size = to - from;
    l.slots.push_back(p);
}

} // namespace

bool flat_layout(const SbeSchema& s, const MessageDef& m, FlatLayout& out, std::string& reason) {
    out = FlatLayout{};
    if (!m.groups.empty()) { reason = "has groups (" + m.groups.front().name + ")"; return false; }
    if (!m.data.empty()) { reason = "has varData (" + m.data.front().name + ")"; return false; }
    out.name = m.name;
    out.template_id = m.template_id;
    const HeaderLayout& h = s.header();
    out.size = static_cast<uint32_t>(h.size) + m.block_length;

    struct HM { const char* n; uint16_t off; uint8_t sz; };
    std::vector<ViewSlot> slots;
    for (const HM& x : {HM{"blockLength", h.block_length_off, h.block_length_size}, HM{"templateId", h.template_id_off, h.template_id_size},
                        HM{"schemaId", h.schema_id_off, h.schema_id_size}, HM{"version", h.version_off, h.version_size}}) {
        ViewSlot v; v.kind = Kind::HEADER; v.name = x.n; v.offset = x.off; v.size = x.sz;
        v.prim = x.sz == 1 ? SbePrim::U8 : x.sz == 2 ? SbePrim::U16 : SbePrim::U32;
        slots.push_back(v);
    }
    for (const FieldDef& f : m.fields) {
        if (f.size == 0) { out.constants.push_back(f.name + " (" + f.type_name + ")"); continue; }
        if (!valid_ident(f.name)) { reason = "field '" + f.name + "' is not an identifier"; return false; }
        ViewSlot v; v.kind = Kind::FIELD; v.name = f.name; v.offset = static_cast<uint32_t>(h.size) + f.offset; v.size = f.size;
        v.prim = f.prim; v.length = f.length; v.elem = f.elem_prim; v.type_name = f.type_name;
        if (v.offset + v.size > out.size) { reason = "field '" + f.name + "' runs past blockLength"; return false; }
        slots.push_back(v);
    }
    std::stable_sort(slots.begin(), slots.end(), [](const ViewSlot& a, const ViewSlot& b) { return a.offset < b.offset; });
    uint32_t cur = 0; int npad = 0;
    for (const ViewSlot& v : slots) {
        if (v.offset < cur) { reason = "field '" + v.name + "' overlaps the previous one"; return false; }
        add_pad(out, cur, v.offset, npad);
        const uint32_t align = cpp_scalar(v.prim) ? v.size : scalar_array(v) ? v.size / v.length : 1;
        if (align > 1 && v.offset % align != 0 && out.aligned) { out.aligned = false; out.misaligned_field = v.name + " at offset " + std::to_string(v.offset); }
        out.slots.push_back(v);
        cur = v.offset + v.size;
    }
    add_pad(out, cur, out.size, npad);
    for (const ViewSlot& v : out.slots) {
        for (const ViewSlot& w : out.slots) {
            if (&v != &w && v.name == w.name) { reason = "duplicate field name '" + v.name + "'"; return false; }
        }
    }
    return true;
}

std::string views_basename(const SbeSchema& s) {
    std::string b;
    for (char c : s.package()) b += std::isalnum(static_cast<unsigned char>(c)) ? c : '_';
    return b.empty() ? "schema" : b;
}

// ------------------------------------------------------------------ C++

bool emit_cpp(const SbeSchema& s, const ViewsOptions& o, std::string& out, ViewsReport& rep, std::string& err) {
    std::vector<FlatLayout> ls;
    if (!collect(s, o, ls, rep, err)) return false;
    const HeaderLayout& h = s.header();
    out.clear();
    out += "#pragma once\n// Generated by ft_schema from " + views_basename(s) + " (schema id " + std::to_string(s.schema_id()) +
           ", version " + std::to_string(s.version()) + "). Do not edit.\n"
           "//\n// Typed views over flat SBE messages. A flashtick ring payload is the whole SBE\n"
           "// message, header first, and starts 64-byte aligned. A message whose fields are\n"
           "// all naturally aligned is a plain struct (the static_asserts prove the layout);\n"
           "// one with a misaligned field is a view class with memcpy accessors, which is\n"
           "// correct on every platform and compiles to the same loads on x86. No #pragma pack.\n"
           "// Use: `if (auto t = " + o.ns + "::view<" + o.ns + "::" + ls.front().name + ">(payload, len)) ...`\n\n"
           "#include <bit>\n#include <cstddef>\n#include <cstdint>\n#include <cstring>\n#include <optional>\n#include <string_view>\n\n"
           "namespace " + o.ns + " {\n\n"
           "static_assert(std::endian::native == std::endian::little, \"these views are little-endian\");\n"
           "inline constexpr uint16_t kSchemaId = " + std::to_string(s.schema_id()) + ";\n"
           "inline constexpr uint16_t kSchemaVersion = " + std::to_string(s.version()) + ";\n"
           "inline constexpr size_t kHeaderSize = " + std::to_string(h.size) + ";\n\n"
           "namespace detail {\ntemplate <class T> inline T load(const uint8_t* p, size_t off) noexcept { T v; std::memcpy(&v, p + off, sizeof(T)); return v; }\n"
           "inline uint32_t load_n(const uint8_t* p, size_t off, size_t n) noexcept { return n == 1 ? load<uint8_t>(p, off) : n == 2 ? load<uint16_t>(p, off) : load<uint32_t>(p, off); }\n"
           "inline bool header_matches(const uint8_t* p, uint32_t len, size_t size, uint16_t template_id) noexcept {\n"
           "    return len >= size && load_n(p, " + std::to_string(h.template_id_off) + ", " + std::to_string(h.template_id_size) + ") == template_id\n"
           "        && load_n(p, " + std::to_string(h.schema_id_off) + ", " + std::to_string(h.schema_id_size) + ") == kSchemaId;\n}\n} // namespace detail\n\n";

    for (const FlatLayout& l : ls) {
        std::string cons;
        for (const auto& c : l.constants) cons += (cons.empty() ? "" : ", ") + c;
        if (l.aligned) {
            out += "// " + l.name + ": template " + std::to_string(l.template_id) + ", " + std::to_string(l.size) + " bytes.";
            if (!cons.empty()) out += " Constant fields omitted: " + cons + ".";
            out += "\nstruct " + l.name + " {\n    static constexpr bool kIsStruct = true;\n"
                   "    static constexpr uint16_t kTemplateId = " + std::to_string(l.template_id) + ";\n"
                   "    static constexpr size_t kSize = " + std::to_string(l.size) + ";\n";
            for (const ViewSlot& v : l.slots) {
                const char* sc = cpp_scalar(v.prim);
                std::string line = "    ";
                if (v.kind == Kind::PAD) line += "uint8_t " + v.name + "[" + std::to_string(v.size) + "];";
                else if (sc) line += std::string(sc) + " " + v.name + ";";
                else if (v.prim == SbePrim::CHAR_ARRAY) line += "char " + v.name + "[" + std::to_string(v.size) + "];";
                else if (scalar_array(v)) line += std::string(cpp_scalar(v.elem)) + " " + v.name + "[" + std::to_string(v.length) + "];";
                else line += "uint8_t " + v.name + "[" + std::to_string(v.size) + "];  // composite bytes";
                const std::string tn = type_note(v);
                if (!tn.empty()) line += "  // " + tn;
                out += line + "\n";
            }
            out += "};\nstatic_assert(sizeof(" + l.name + ") == " + l.name + "::kSize);\n";
            for (const ViewSlot& v : l.slots)
                if (v.kind != Kind::PAD) out += "static_assert(offsetof(" + l.name + ", " + v.name + ") == " + std::to_string(v.offset) + ");\n";
            out += "\n";
        } else {
            out += "// " + l.name + ": template " + std::to_string(l.template_id) + ", " + std::to_string(l.size) +
                   " bytes. View class, not a struct: " + l.misaligned_field + " is not naturally aligned.";
            if (!cons.empty()) out += " Constant fields omitted: " + cons + ".";
            out += "\nclass " + l.name + " {\npublic:\n    static constexpr bool kIsStruct = false;\n"
                   "    static constexpr uint16_t kTemplateId = " + std::to_string(l.template_id) + ";\n"
                   "    static constexpr size_t kSize = " + std::to_string(l.size) + ";\n"
                   "    explicit " + l.name + "(const uint8_t* p) noexcept : p_(p) {}\n"
                   "    const uint8_t* data() const noexcept { return p_; }\n";
            for (const ViewSlot& v : l.slots) {
                if (v.kind == Kind::PAD) continue;
                const char* sc = cpp_scalar(v.prim);
                std::string line;
                if (sc) line = "    " + std::string(sc) + " " + v.name + "() const noexcept { return detail::load<" + sc + ">(p_, " + std::to_string(v.offset) + "); }";
                else if (v.prim == SbePrim::CHAR_ARRAY) line = "    std::string_view " + v.name + "() const noexcept { return {reinterpret_cast<const char*>(p_ + " + std::to_string(v.offset) + "), " + std::to_string(v.size) + "}; }";
                else if (scalar_array(v)) line = "    static constexpr size_t k" + v.name + "Len = " + std::to_string(v.length) + ";\n    " + std::string(cpp_scalar(v.elem)) + " " + v.name + "(size_t i) const noexcept { return detail::load<" + cpp_scalar(v.elem) + ">(p_, " + std::to_string(v.offset) + " + i * " + std::to_string(v.size / v.length) + "); }";
                else line = "    const uint8_t* " + v.name + "() const noexcept { return p_ + " + std::to_string(v.offset) + "; }  // " + std::to_string(v.size) + " composite bytes";
                const std::string tn = type_note(v);
                if (!tn.empty()) line += "  // " + tn;
                out += line + "\n";
            }
            out += "private:\n    const uint8_t* p_;\n};\n\n";
        }
    }
    out += "// Struct form: returns a pointer into the payload, or nullptr. View form: returns an\n"
           "// optional view. Both check length, template id and schema id first.\n"
           "template <class T>\ninline auto view(const uint8_t* payload, uint32_t len) noexcept {\n"
           "    if constexpr (T::kIsStruct) {\n"
           "        return detail::header_matches(payload, len, T::kSize, T::kTemplateId) ? reinterpret_cast<const T*>(payload) : nullptr;\n"
           "    } else {\n"
           "        return detail::header_matches(payload, len, T::kSize, T::kTemplateId) ? std::optional<T>(T(payload)) : std::nullopt;\n"
           "    }\n}\n\n} // namespace " + o.ns + "\n";
    return true;
}

// ------------------------------------------------------------------ Python

namespace {
// struct module format char for one slot (padding is 'Nx').
std::string py_fmt(const ViewSlot& v) {
    if (v.kind == Kind::PAD) return std::to_string(v.size) + "x";
    switch (v.prim) {
        case SbePrim::U8: return "B";  case SbePrim::I8: return "b";
        case SbePrim::U16: return "H"; case SbePrim::I16: return "h";
        case SbePrim::U32: return "I"; case SbePrim::I32: return "i";
        case SbePrim::U64: return "Q"; case SbePrim::I64: return "q";
        case SbePrim::F32: return "f"; case SbePrim::F64: return "d";
        default: return std::to_string(v.size) + "s";
    }
}
// Scalar arrays are unpacked as a tuple: FORMAT repeats the element code `length` times.
std::string py_fmt_elem(SbePrim p) {
    ViewSlot t; t.kind = Kind::FIELD; t.prim = p; t.size = 1;
    return py_fmt(t);
}
const char* py_type(const ViewSlot& v) {
    if (scalar_array(v)) return "tuple";
    switch (v.prim) {
        case SbePrim::F32: case SbePrim::F64: return "float";
        case SbePrim::CHAR_ARRAY: case SbePrim::OPAQUE: return "bytes";
        default: return "int";
    }
}
} // namespace

bool emit_numpy(const SbeSchema& s, const ViewsOptions& o, std::string& out, ViewsReport& rep, std::string& err) {
    std::vector<FlatLayout> ls;
    if (!collect(s, o, ls, rep, err)) return false;
    out.clear();
    out += "# Generated by ft_schema from " + views_basename(s) + " (schema id " + std::to_string(s.schema_id()) + ", version " +
           std::to_string(s.version()) + "). Do not edit.\n"
           "# One frozen dataclass per flat SBE message (header fields first), built with the standard\n"
           "# library's struct module: `msg = Trade.from_bytes(payload)`. `parse(record)` turns one ring\n"
           "# record (64-byte header + payload) into (RecordHeader, message or None). With NumPy present,\n"
           "# each class also carries a structured DTYPE with explicit offsets for columnar, zero-copy reads:\n"
           "# `np.frombuffer(payloads, dtype=Trade.DTYPE)`. Little-endian.\n"
           "from dataclasses import dataclass\nimport struct\ntry:\n    import numpy as np\nexcept ImportError:  # dataclasses work without NumPy\n    np = None\n\n"
           "SCHEMA_ID = " + std::to_string(s.schema_id()) + "\nSCHEMA_VERSION = " + std::to_string(s.version()) + "\nHEADER_SIZE = " + std::to_string(s.header().size) + "\n\n"
           "def _regroup(flat, arrays, slots):\n"
           "    \"\"\"struct unpacks scalar arrays flat; regroup them into tuples in field order.\"\"\"\n"
           "    lens = dict(arrays)\n    out, i = [], 0\n"
           "    for name in slots:\n        n = lens.get(name)\n"
           "        if n is None:\n            out.append(flat[i]); i += 1\n"
           "        else:\n            out.append(tuple(flat[i:i + n])); i += n\n"
           "    return out\n\n"
           "@dataclass(frozen=True, slots=True)\nclass RecordHeader:\n"
           "    \"\"\"flashtick ring record header (core/message.h TickHeader), 64 bytes.\"\"\"\n"
           "    seq_tag: int\n    sequence: int\n    exchange_ts: int\n    ingest_ts: int\n    symbol_id: int\n    payload_len: int\n    template_id: int\n"
           "    schema_id: int\n    schema_version: int\n    venue_id: int\n    partition_id: int\n    msg_type: int\n    kind: int\n    wire_format: int\n    flags: int\n"
           "    FORMAT = '<QQQQIIHHHHHBBBB10x'\n    SIZE = 64\n    KIND_DATA = 0\n\n"
           "    @classmethod\n    def from_bytes(cls, b, offset=0):\n        return cls(*struct.unpack_from(cls.FORMAT, b, offset))\n\n"
           "if np is not None:\n"
           "    RECORD_HEADER = np.dtype({'names': ['seq_tag', 'sequence', 'exchange_ts', 'ingest_ts', 'symbol_id', 'payload_len', 'template_id',\n"
           "                                        'schema_id', 'schema_version', 'venue_id', 'partition_id', 'msg_type', 'kind', 'wire_format', 'flags'],\n"
           "                              'formats': ['<u8', '<u8', '<u8', '<u8', '<u4', '<u4', '<u2', '<u2', '<u2', '<u2', '<u2', 'u1', 'u1', 'u1', 'u1'],\n"
           "                              'offsets': [0, 8, 16, 24, 32, 36, 40, 42, 44, 46, 48, 50, 51, 52, 53],\n"
           "                              'itemsize': 64})\n"
           "    RecordHeader.DTYPE = RECORD_HEADER\n\n";
    std::string ids, byid;
    for (const FlatLayout& l : ls) {
        std::string cons;
        for (const auto& c : l.constants) cons += (cons.empty() ? "" : ", ") + c;
        std::string fmt = "<", fields, names, fmts, offs, arrays;
        for (const ViewSlot& v : l.slots) {
            if (scalar_array(v)) { fmt += std::to_string(v.length) + py_fmt_elem(v.elem); arrays += (arrays.empty() ? "" : ", ") + ("('" + v.name + "', " + std::to_string(v.length) + ")"); }
            else fmt += py_fmt(v);
            if (v.kind == Kind::PAD) continue;
            fields += "    " + v.name + ": " + py_type(v);
            const std::string tn = type_note(v);
            fields += (tn.empty() ? "" : "  # " + tn) + "\n";
            names += (names.empty() ? "" : ", ") + ("'" + v.name + "'");
            fmts += (fmts.empty() ? "" : ", ") + (scalar_array(v) ? np_fmt(v) : "'" + np_fmt(v) + "'");
            offs += (offs.empty() ? "" : ", ") + std::to_string(v.offset);
        }
        out += "@dataclass(frozen=True, slots=True)\nclass " + l.name + ":\n    \"\"\"template " + std::to_string(l.template_id) + ", " +
               std::to_string(l.size) + " bytes" + (cons.empty() ? "" : "; constant fields omitted: " + cons) + ".\"\"\"\n" + fields +
               "    TEMPLATE_ID = " + std::to_string(l.template_id) + "\n    FORMAT = '" + fmt + "'\n    SIZE = " + std::to_string(l.size) + "\n\n" +
               (arrays.empty()
                    ? "    @classmethod\n    def from_bytes(cls, b, offset=0):\n        return cls(*struct.unpack_from(cls.FORMAT, b, offset))\n\n"
                    : "    ARRAYS = [" + arrays + "]  # (field, length): unpacked flat, regrouped as tuples\n\n"
                      "    @classmethod\n    def from_bytes(cls, b, offset=0):\n        return cls(*_regroup(struct.unpack_from(cls.FORMAT, b, offset), cls.ARRAYS, cls.__slots__))\n\n") +
               "if np is not None:\n    " + l.name + ".DTYPE = np.dtype({'names': [" + names + "],\n        'formats': [" + fmts + "],\n        'offsets': [" + offs + "],\n        'itemsize': " +
               std::to_string(l.size) + "})\n\n";
        ids += (ids.empty() ? "" : ", ") + ("'" + l.name + "': " + std::to_string(l.template_id));
        byid += (byid.empty() ? "" : ", ") + (std::to_string(l.template_id) + ": " + l.name);
    }
    out += "TEMPLATE_ID = {" + ids + "}\nBY_TEMPLATE = {" + byid + "}\n\n"
           "def message_from_bytes(payload, offset=0):\n"
           "    \"\"\"The dataclass for one SBE message (header first), or None for a template not in this module.\"\"\"\n"
           "    template_id, schema_id = struct.unpack_from('<" + std::string(s.header().template_id_size == 2 ? "H" : s.header().template_id_size == 1 ? "B" : "I") +
           "', payload, offset + " + std::to_string(s.header().template_id_off) + ")[0], struct.unpack_from('<" +
           std::string(s.header().schema_id_size == 2 ? "H" : s.header().schema_id_size == 1 ? "B" : "I") + "', payload, offset + " + std::to_string(s.header().schema_id_off) + ")[0]\n"
           "    cls = BY_TEMPLATE.get(template_id)\n"
           "    if cls is None or schema_id != SCHEMA_ID or len(payload) - offset < cls.SIZE:\n        return None\n"
           "    return cls.from_bytes(payload, offset)\n\n"
           "def parse(record, offset=0):\n"
           "    \"\"\"One ring record (RecordHeader + payload) -> (RecordHeader, message or None).\"\"\"\n"
           "    hdr = RecordHeader.from_bytes(record, offset)\n"
           "    msg = message_from_bytes(record, offset + RecordHeader.SIZE) if hdr.kind == RecordHeader.KIND_DATA else None\n"
           "    return hdr, msg\n";
    return true;
}

// ------------------------------------------------------------------ q

bool emit_q(const SbeSchema& s, const ViewsOptions& o, std::string& out, ViewsReport& rep, std::string& err) {
    std::vector<FlatLayout> ls;
    if (!collect(s, o, ls, rep, err)) return false;
    const std::string base = views_basename(s);
    out.clear();
    out += "/ Generated by ft_schema from " + base + " (schema id " + std::to_string(s.schema_id()) + ", version " + std::to_string(s.version()) + "). Do not edit.\n"
           "/ Fixed-width specs for `1:` over flat SBE messages (header first, little-endian).\n"
           "/ Unsigned fields are read as the signed q type of the same width (u8 x, u16 h, u32 i, u64 j).\n"
           "/ Char arrays and composite bytes are skipped by the spec and extracted by .parse from the raw bytes.\n"
           "/ .ft." + base + ".<Message>.parse takes one message's bytes and returns a dictionary.\n\n";
    for (const FlatLayout& l : ls) {
        std::string names, types, widths, chars;
        for (const ViewSlot& v : l.slots) {
            const char t = v.kind == Kind::PAD ? ' ' : q_type(v.prim);
            types += t;
            widths += (widths.empty() ? "" : " ") + std::to_string(v.size);
            if (v.kind == Kind::PAD) continue;
            if (t == ' ') {
                if (v.prim == SbePrim::CHAR_ARRAY) chars += " d[`" + v.name + "]:`$\"c\"$b[" + std::to_string(v.offset) + "+til " + std::to_string(v.size) + "];";
                else if (scalar_array(v)) chars += " d[`" + v.name + "]:first (enlist \"" + std::string(1, q_type(v.elem)) + "\";enlist " + std::to_string(v.size / v.length) + ") 1: b[" + std::to_string(v.offset) + "+til " + std::to_string(v.size) + "];";
                else chars += " d[`" + v.name + "]:b[" + std::to_string(v.offset) + "+til " + std::to_string(v.size) + "];";
            } else {
                names += "`" + v.name;
            }
        }
        const std::string p = ".ft." + base + "." + l.name;
        out += "/ template " + std::to_string(l.template_id) + ", " + std::to_string(l.size) + " bytes\n" +
               p + ".templateId:" + std::to_string(l.template_id) + "\n" +
               p + ".size:" + std::to_string(l.size) + "\n" +
               p + ".names:" + names + "\n" +
               p + ".spec:(\"" + types + "\";" + widths + ")\n" +
               p + ".parse:{[b] d:" + p + ".names!first each " + p + ".spec 1: b;" + chars + " d}\n\n";
    }
    return true;
}

} // namespace flashtick
