#include "subplane/decoder/sbe_codec.h"

#include <sstream>

namespace subplane {
namespace {

const char* scalar_type(SbePrim p) {
    switch (p) {
        case SbePrim::U8: return "uint8_t";   case SbePrim::I8: return "int8_t";
        case SbePrim::U16: return "uint16_t"; case SbePrim::I16: return "int16_t";
        case SbePrim::U32: return "uint32_t"; case SbePrim::I32: return "int32_t";
        case SbePrim::U64: return "uint64_t"; case SbePrim::I64: return "int64_t";
        case SbePrim::F32: return "float";    case SbePrim::F64: return "double";
        default: return nullptr;
    }
}

const char* len_type(uint8_t size) {
    switch (size) { case 1: return "uint8_t"; case 2: return "uint16_t"; default: return "uint32_t"; }
}

bool check_fields(const SbeSchema& s, const std::string& where, const std::vector<FieldDef>& fields, std::string& err) {
    for (const auto& f : fields) {
        if (f.size == 0) continue; // constant
        if (f.prim == SbePrim::CHAR_ARRAY) continue;
        if (scalar_type(f.prim) == nullptr) {
            err = where + "." + f.name + ": composite and scalar-array fields are not supported";
            return false;
        }
        if (const EnumDef* e = s.find_enum(f.type_name); e != nullptr && scalar_type(e->encoding) == nullptr) {
            err = where + "." + f.name + ": enum with a char encoding is not supported";
            return false;
        }
    }
    return true;
}

bool check(const SbeSchema& s, std::string& err) {
    if (!s.little_endian()) { err = "big-endian schema: the codec is little-endian only"; return false; }
    const HeaderLayout& h = s.header();
    if (h.size != 8 || h.block_length_off != 0 || h.template_id_off != 2 || h.schema_id_off != 4 || h.version_off != 6 ||
        h.block_length_size != 2 || h.template_id_size != 2 || h.schema_id_size != 2 || h.version_size != 2) {
        err = "the codec needs the standard 8-byte message header (blockLength, templateId, schemaId, version as uint16)";
        return false;
    }
    for (const auto& m : s.messages()) {
        if (!check_fields(s, m.name, m.fields, err)) return false;
        for (const auto& g : m.groups) {
            if (!g.groups.empty() || !g.data.empty()) {
                err = m.name + "." + g.name + ": groups with nested groups or data are not supported";
                return false;
            }
            if (g.dim.size != 4 || g.dim.block_length_off != 0 || g.dim.num_in_group_off != 2 ||
                g.dim.block_length_size != 2 || g.dim.num_in_group_size != 2) {
                err = m.name + "." + g.name + ": the codec needs a groupSizeEncoding of uint16 blockLength, uint16 numInGroup";
                return false;
            }
            if (!check_fields(s, m.name + "." + g.name, g.fields, err)) return false;
        }
        for (const auto& d : m.data) {
            if (d.len_off != 0) { err = m.name + "." + d.name + ": the varData length must come first"; return false; }
        }
    }
    return true;
}

// Return type of a getter / parameter type of a setter.
std::string value_type(const SbeSchema& s, const std::string& ns, const FieldDef& f) {
    if (s.find_enum(f.type_name) != nullptr) return "::" + ns + "::" + f.type_name;
    return scalar_type(f.prim);
}

void emit_getters(std::ostringstream& o, const SbeSchema& s, const std::string& ns, const std::vector<FieldDef>& fields,
                  const std::string& base, const std::string& block, const std::string& version, const char* indent) {
    for (const auto& f : fields) {
        if (f.size == 0) continue;
        const std::string present = std::to_string(f.offset + f.size) + "u <= " + block +
                                    (f.since_version != 0 ? " && " + version + " >= " + std::to_string(f.since_version) : "");
        if (f.prim == SbePrim::CHAR_ARRAY) {
            o << indent << "// " << f.name << ": char[" << f.length << "], NUL-padded; empty when absent.\n";
            o << indent << "std::string_view " << f.name << "() const noexcept {\n"
              << indent << "    if (!(" << present << ")) return {};\n"
              << indent << "    const char* c = reinterpret_cast<const char*>(" << base << " + " << f.offset << ");\n"
              << indent << "    size_t n = 0;\n"
              << indent << "    while (n < " << f.length << "u && c[n] != '\\0') ++n;\n"
              << indent << "    return {c, n};\n"
              << indent << "}\n";
            continue;
        }
        const std::string vt = value_type(s, ns, f);
        const char* st = scalar_type(f.prim);
        o << indent << vt << " " << f.name << "() const noexcept { return " << present << " ? ";
        if (s.find_enum(f.type_name) != nullptr) o << "static_cast<" << vt << ">(detail::load<" << st << ">(" << base << " + " << f.offset << "))";
        else o << "detail::load<" << st << ">(" << base << " + " << f.offset << ")";
        o << " : " << vt << "{}; }\n";
    }
}

void emit_setters(std::ostringstream& o, const SbeSchema& s, const std::string& ns, const std::vector<FieldDef>& fields,
                  const std::string& self, const std::string& base, const std::string& guard, const char* indent) {
    for (const auto& f : fields) {
        if (f.size == 0) continue;
        if (f.prim == SbePrim::CHAR_ARRAY) {
            o << indent << "// Longer than " << f.length << " chars fails the message.\n";
            o << indent << self << "& " << f.name << "(std::string_view v) noexcept {\n"
              << indent << "    if (" << guard << ") { if (v.size() > " << f.length << "u) ok_ = false; else std::memcpy(" << base << " + " << f.offset << ", v.data(), v.size()); }\n"
              << indent << "    return *this;\n"
              << indent << "}\n";
            continue;
        }
        const std::string vt = value_type(s, ns, f);
        const char* st = scalar_type(f.prim);
        o << indent << self << "& " << f.name << "(" << vt << " v) noexcept { if (" << guard << ") detail::store<" << st << ">(" << base << " + " << f.offset
          << ", static_cast<" << st << ">(v)); return *this; }\n";
    }
}

void emit_message(std::ostringstream& o, const SbeSchema& s, const std::string& ns, const MessageDef& m) {
    const size_t ng = m.groups.size(), nd = m.data.size();
    o << "// ---- " << m.name << " (template " << m.template_id << ") ----\n\n";

    // Group entry types.
    for (const auto& g : m.groups) {
        o << "class " << m.name << g.name << "Entry {\n"
          << "public:\n"
          << "    static constexpr uint16_t kBlockLength = " << g.block_length << ";\n"
          << "    " << m.name << g.name << "Entry(uint8_t* p) noexcept : p_(p) {}\n";
        // Setters write through p_ (nullptr after an overflow).
        std::ostringstream tmp;
        emit_setters(tmp, s, ns, g.fields, m.name + g.name + "Entry", "p_", "p_ != nullptr", "    ");
        std::string body = tmp.str();
        // Entries have no ok_; a too-long char array leaves the field empty.
        for (size_t at = body.find("ok_ = false"); at != std::string::npos; at = body.find("ok_ = false", at)) body.replace(at, 11, "(void)0");
        o << body
          << "private:\n"
          << "    uint8_t* p_;\n"
          << "};\n\n";
        o << "class " << m.name << g.name << "View {\n"
          << "public:\n"
          << "    " << m.name << g.name << "View(const uint8_t* p, uint16_t block) noexcept : p_(p), block_(block) {}\n";
        emit_getters(o, s, ns, g.fields, "p_", "block_", "kSchemaVersion", "    ");
        o << "private:\n"
          << "    const uint8_t* p_;\n"
          << "    uint16_t block_;\n"
          << "};\n\n";
    }

    // Encoder.
    o << "class " << m.name << "Encoder {\n"
      << "public:\n"
      << "    static constexpr uint16_t kTemplateId = " << m.template_id << ";\n"
      << "    static constexpr uint16_t kBlockLength = " << m.block_length << ";\n\n"
      << "    // Writes the header and a zeroed root block. Set root fields in any order,\n"
      << "    // then groups and data in schema order, then call finish().\n"
      << "    " << m.name << "Encoder(uint8_t* buf, size_t cap) noexcept : buf_(buf), cap_(cap) {\n"
      << "        if (cap < kHeaderSize + kBlockLength) { ok_ = false; return; }\n"
      << "        std::memset(buf, 0, kHeaderSize + kBlockLength);\n"
      << "        detail::store<uint16_t>(buf + 0, kBlockLength);\n"
      << "        detail::store<uint16_t>(buf + 2, kTemplateId);\n"
      << "        detail::store<uint16_t>(buf + 4, kSchemaId);\n"
      << "        detail::store<uint16_t>(buf + 6, kSchemaVersion);\n"
      << "        pos_ = kHeaderSize + kBlockLength;\n"
      << "    }\n\n";
    emit_setters(o, s, ns, m.fields, m.name + "Encoder", "buf_ + kHeaderSize", "ok_", "    ");
    for (size_t i = 0; i < ng; ++i) {
        const GroupDef& g = m.groups[i];
        o << "\n    // " << g.name << ": reserves n zeroed entries; fill them with " << g.name << "(i).\n"
          << "    " << m.name << "Encoder& begin_" << g.name << "(uint16_t n) noexcept {\n"
          << "        advance(" << i << ");\n"
          << "        if (!ok_) return *this;\n"
          << "        const size_t need = 4u + static_cast<size_t>(n) * " << m.name << g.name << "Entry::kBlockLength;\n"
          << "        if (pos_ + need > cap_) { ok_ = false; return *this; }\n"
          << "        detail::store<uint16_t>(buf_ + pos_, " << m.name << g.name << "Entry::kBlockLength);\n"
          << "        detail::store<uint16_t>(buf_ + pos_ + 2, n);\n"
          << "        std::memset(buf_ + pos_ + 4, 0, need - 4u);\n"
          << "        g" << i << "_ = buf_ + pos_ + 4;\n"
          << "        n" << i << "_ = n;\n"
          << "        pos_ += need;\n"
          << "        stage_ = " << i + 1 << ";\n"
          << "        return *this;\n"
          << "    }\n"
          << "    " << m.name << g.name << "Entry " << g.name << "(uint16_t i) noexcept {\n"
          << "        return " << m.name << g.name << "Entry(ok_ && i < n" << i << "_ ? g" << i << "_ + static_cast<size_t>(i) * "
          << m.name << g.name << "Entry::kBlockLength : nullptr);\n"
          << "    }\n";
    }
    for (size_t j = 0; j < nd; ++j) {
        const VarDataDef& d = m.data[j];
        const char* lt = len_type(d.len_size);
        o << "\n    // " << d.name << ": " << lt << " length, then the bytes.\n"
          << "    " << m.name << "Encoder& " << d.name << "(const void* p, size_t n) noexcept {\n"
          << "        advance(" << ng + j << ");\n"
          << "        if (!ok_) return *this;\n"
          << "        if (n > static_cast<size_t>(static_cast<" << lt << ">(~" << lt << "{0})) || pos_ + " << +d.hdr_size << "u + n > cap_) { ok_ = false; return *this; }\n"
          << "        detail::store<" << lt << ">(buf_ + pos_, static_cast<" << lt << ">(n));\n"
          << "        if (n != 0) std::memcpy(buf_ + pos_ + " << +d.hdr_size << ", p, n);\n"
          << "        pos_ += " << +d.hdr_size << "u + n;\n"
          << "        stage_ = " << ng + j + 1 << ";\n"
          << "        return *this;\n"
          << "    }\n"
          << "    " << m.name << "Encoder& " << d.name << "(std::string_view v) noexcept { return " << d.name << "(v.data(), v.size()); }\n";
    }
    o << "\n    // Writes every group and data element not yet written, empty. Returns the\n"
      << "    // message length, or 0 when the buffer was too small or a call was out of order.\n"
      << "    size_t finish() noexcept { advance(" << ng + nd << "); return ok_ ? pos_ : 0; }\n"
      << "    bool ok() const noexcept { return ok_; }\n\n"
      << "private:\n"
      << "    // INTENT: SBE requires every group and data element on the wire, in\n"
      << "    // schema order. Elements before `to` that were not written go out empty;\n"
      << "    // going back to an earlier element fails the message.\n"
      << "    void advance(size_t to) noexcept {\n"
      << "        if (stage_ > to) { ok_ = false; return; }\n"
      << "        while (ok_ && stage_ < to) {\n";
    if (ng + nd == 0) {
        o << "            ++stage_;\n";
    } else {
        o << "            switch (stage_) {\n";
        for (size_t i = 0; i < ng; ++i) {
            o << "                case " << i << ": if (pos_ + 4u > cap_) { ok_ = false; return; } detail::store<uint16_t>(buf_ + pos_, "
              << m.name << m.groups[i].name << "Entry::kBlockLength); detail::store<uint16_t>(buf_ + pos_ + 2, 0); pos_ += 4u; break;\n";
        }
        for (size_t j = 0; j < nd; ++j) {
            const VarDataDef& d = m.data[j];
            o << "                case " << ng + j << ": if (pos_ + " << +d.hdr_size << "u > cap_) { ok_ = false; return; } std::memset(buf_ + pos_, 0, "
              << +d.hdr_size << "u); pos_ += " << +d.hdr_size << "u; break;\n";
        }
        o << "                default: break;\n"
          << "            }\n"
          << "            ++stage_;\n";
    }
    o << "        }\n"
      << "    }\n\n"
      << "    uint8_t* buf_;\n"
      << "    size_t   cap_;\n"
      << "    size_t   pos_ = 0;\n"
      << "    size_t   stage_ = 0;\n"
      << "    bool     ok_ = true;\n";
    for (size_t i = 0; i < ng; ++i) o << "    uint8_t* g" << i << "_ = nullptr;\n    uint16_t n" << i << "_ = 0;\n";
    o << "};\n\n";

    // Decoder.
    o << "class " << m.name << "Decoder {\n"
      << "public:\n"
      << "    static constexpr uint16_t kTemplateId = " << m.template_id << ";\n\n"
      << "    // INTENT: every length is checked against `len` here, once; the getters\n"
      << "    // then read only inside the checked bounds. Input comes off the wire.\n"
      << "    bool wrap(const uint8_t* p, size_t len) noexcept {\n"
      << "        p_ = nullptr;\n"
      << "        Header h;\n"
      << "        if (!read_header(p, len, h) || h.schema_id != kSchemaId || h.template_id != kTemplateId) return false;\n"
      << "        size_t pos = kHeaderSize + h.block_length;\n"
      << "        if (pos > len) return false;\n"
      << "        version_ = h.version;\n"
      << "        block_ = h.block_length;\n";
    for (size_t i = 0; i < ng; ++i) {
        const GroupDef& g = m.groups[i];
        const std::string cond = g.since_version != 0 ? "version_ >= " + std::to_string(g.since_version) : "true";
        o << "        g" << i << "_ = nullptr; n" << i << "_ = 0; b" << i << "_ = 0;\n"
          << "        if (" << cond << ") {\n"
          << "            if (pos + 4u > len) return false;\n"
          << "            b" << i << "_ = detail::load<uint16_t>(p + pos);\n"
          << "            n" << i << "_ = detail::load<uint16_t>(p + pos + 2);\n"
          << "            pos += 4u;\n"
          << "            const size_t bytes = static_cast<size_t>(b" << i << "_) * n" << i << "_;\n"
          << "            if (pos + bytes > len) return false;\n"
          << "            g" << i << "_ = p + pos;\n"
          << "            pos += bytes;\n"
          << "        }\n";
    }
    for (size_t j = 0; j < nd; ++j) {
        const VarDataDef& d = m.data[j];
        const std::string cond = d.since_version != 0 ? "version_ >= " + std::to_string(d.since_version) : "true";
        o << "        d" << j << "_ = nullptr; dn" << j << "_ = 0;\n"
          << "        if (" << cond << ") {\n"
          << "            if (pos + " << +d.hdr_size << "u > len) return false;\n"
          << "            const size_t n = detail::load<" << len_type(d.len_size) << ">(p + pos);\n"
          << "            pos += " << +d.hdr_size << "u;\n"
          << "            if (pos + n > len) return false;\n"
          << "            d" << j << "_ = p + pos;\n"
          << "            dn" << j << "_ = n;\n"
          << "            pos += n;\n"
          << "        }\n";
    }
    o << "        p_ = p;\n"
      << "        size_ = pos;\n"
      << "        return true;\n"
      << "    }\n\n"
      << "    // Bytes the known elements occupy (a newer sender may append more).\n"
      << "    size_t size() const noexcept { return size_; }\n"
      << "    uint16_t version() const noexcept { return version_; }\n"
      << "    uint16_t block_length() const noexcept { return block_; }\n\n";
    emit_getters(o, s, ns, m.fields, "p_ + kHeaderSize", "block_", "version_", "    ");
    for (size_t i = 0; i < ng; ++i) {
        const GroupDef& g = m.groups[i];
        o << "    uint16_t " << g.name << "_count() const noexcept { return n" << i << "_; }\n"
          << "    // i < " << g.name << "_count().\n"
          << "    " << m.name << g.name << "View " << g.name << "(uint16_t i) const noexcept { return " << m.name << g.name << "View(g" << i
          << "_ + static_cast<size_t>(i) * b" << i << "_, b" << i << "_); }\n";
    }
    for (size_t j = 0; j < nd; ++j) {
        const VarDataDef& d = m.data[j];
        o << "    const uint8_t* " << d.name << "_data() const noexcept { return d" << j << "_; }\n"
          << "    size_t " << d.name << "_size() const noexcept { return dn" << j << "_; }\n"
          << "    std::string_view " << d.name << "() const noexcept { return d" << j << "_ ? std::string_view(reinterpret_cast<const char*>(d"
          << j << "_), dn" << j << "_) : std::string_view(); }\n";
    }
    o << "\nprivate:\n"
      << "    const uint8_t* p_ = nullptr;\n"
      << "    size_t   size_ = 0;\n"
      << "    uint16_t version_ = 0;\n"
      << "    uint16_t block_ = 0;\n";
    for (size_t i = 0; i < ng; ++i) o << "    const uint8_t* g" << i << "_ = nullptr;\n    uint16_t n" << i << "_ = 0, b" << i << "_ = 0;\n";
    for (size_t j = 0; j < nd; ++j) o << "    const uint8_t* d" << j << "_ = nullptr;\n    size_t dn" << j << "_ = 0;\n";
    o << "};\n\n";
}

} // namespace

bool emit_cpp_codec(const SbeSchema& s, const CodecOptions& opt, std::string& out, std::string& err) {
    if (!check(s, err)) return false;
    std::ostringstream o;
    o << "// Generated by `subplane schema --codec cpp` from schema " << (s.package().empty() ? "(unnamed)" : s.package())
      << " (id " << s.schema_id() << ", version " << s.version() << "). Do not edit.\n"
      << "// Encoders write into a caller buffer and never allocate; decoders check\n"
      << "// every length once in wrap(). Little-endian hosts only.\n"
      << "#pragma once\n\n"
      << "#include <cstddef>\n#include <cstdint>\n#include <cstring>\n#include <string_view>\n\n"
      << "namespace " << opt.ns << " {\n\n"
      << "inline constexpr uint16_t kSchemaId = " << s.schema_id() << ";\n"
      << "inline constexpr uint16_t kSchemaVersion = " << s.version() << ";\n"
      << "inline constexpr size_t   kHeaderSize = 8;\n\n";
    for (const auto& e : s.enums()) {
        o << "enum class " << e.name << " : " << scalar_type(e.encoding) << " {\n";
        for (const auto& [n, v] : e.values) o << "    " << n << " = " << v << ",\n";
        o << "};\n";
    }
    o << "\nnamespace detail {\n"
      << "template <class T> inline T load(const uint8_t* p) noexcept { T v; std::memcpy(&v, p, sizeof(T)); return v; }\n"
      << "template <class T> inline void store(uint8_t* p, T v) noexcept { std::memcpy(p, &v, sizeof(T)); }\n"
      << "} // namespace detail\n\n"
      << "struct Header {\n"
      << "    uint16_t block_length = 0, template_id = 0, schema_id = 0, version = 0;\n"
      << "};\n"
      << "// False when `len` is shorter than a header.\n"
      << "inline bool read_header(const uint8_t* p, size_t len, Header& h) noexcept {\n"
      << "    if (p == nullptr || len < kHeaderSize) return false;\n"
      << "    h.block_length = detail::load<uint16_t>(p);\n"
      << "    h.template_id = detail::load<uint16_t>(p + 2);\n"
      << "    h.schema_id = detail::load<uint16_t>(p + 4);\n"
      << "    h.version = detail::load<uint16_t>(p + 6);\n"
      << "    return true;\n"
      << "}\n\n";
    for (const auto& m : s.messages()) emit_message(o, s, opt.ns, m);
    o << "} // namespace " << opt.ns << "\n";
    out = o.str();
    return true;
}

} // namespace subplane
