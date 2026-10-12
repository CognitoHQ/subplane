#pragma once

// SBE schema loader (FIX SBE 1.0/2.0 XML). COLD path only: parses the XML via
// pugixml and exposes the layout facts the walker and routing decoder need.
// Nothing here is touched per message.

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace flashtick {

enum class SbePrim : uint8_t {
    U8, U16, U32, U64, I8, I16, I32, I64,
    CHAR_ARRAY, // char, or char[N]
    F32, F64,
    OPAQUE,     // composite or non-char array: occupies bytes, not readable as a scalar
};

struct FieldDef {
    std::string name;
    uint16_t    id     = 0;
    uint16_t    offset = 0;
    uint16_t    size   = 0;      // bytes occupied in the block (0 for constants)
    SbePrim     prim   = SbePrim::U8;
    uint16_t    length = 1;      // element count for CHAR_ARRAY
    std::string type_name;       // the XML `type` attribute (enum/set/type name or primitive); cold, for tooling
};

// Layout of the message header composite (derived from headerType).
struct HeaderLayout {
    uint16_t block_length_off = 0, template_id_off = 0, schema_id_off = 0, version_off = 0;
    uint8_t  block_length_size = 0, template_id_size = 0, schema_id_size = 0, version_size = 0;
    uint16_t size = 0;
};

// Group dimension composite (groupSizeEncoding-like).
struct DimensionDef {
    uint16_t size = 0;           // total bytes of the dimension header
    uint16_t block_length_off = 0, num_in_group_off = 0;
    uint8_t  block_length_size = 0, num_in_group_size = 0;
};

// Variable-length data: a length prefix then that many bytes.
struct VarDataDef {
    std::string name;
    uint16_t    id       = 0;
    uint16_t    len_off  = 0;
    uint8_t     len_size = 0;
    uint16_t    hdr_size = 0;    // bytes before the data (length member and siblings)
    uint16_t    since_version = 0; // absent on the wire when the message's acting version is older
};

struct GroupDef {
    std::string            name;
    uint16_t               id           = 0;
    DimensionDef           dim;
    uint16_t               block_length = 0;  // schema's entry size
    uint16_t               since_version = 0; // absent on the wire when the message's acting version is older
    std::vector<FieldDef>  fields;
    std::vector<GroupDef>  groups;
    std::vector<VarDataDef> data;
};

struct MessageDef {
    std::string             name;
    uint16_t                template_id  = 0;
    uint16_t                block_length = 0; // schema's root block size
    std::vector<FieldDef>   fields;           // root-block fields only
    std::vector<GroupDef>   groups;
    std::vector<VarDataDef> data;
};

class SbeSchema {
public:
    bool load_file(const std::string& path, std::string& err);
    bool load_string(std::string_view xml, std::string& err);

    uint16_t            schema_id() const noexcept { return schema_id_; }
    const std::string&  package() const noexcept { return package_; }   // XML `package` attribute, may be empty
    uint16_t            version() const noexcept { return version_; }
    bool                little_endian() const noexcept { return little_endian_; }
    const HeaderLayout& header() const noexcept { return header_; }
    const std::vector<MessageDef>& messages() const noexcept { return messages_; }

    const MessageDef* find_message(uint16_t template_id) const;
    // Root-block fields only.
    const FieldDef*   find_field(const MessageDef& m, std::string_view name) const;
    // True if `name` is a group/data/field living inside a group or a data member.
    bool              is_nested_field(const MessageDef& m, std::string_view name) const;

private:
    uint16_t schema_id_ = 0, version_ = 0;
    std::string package_;
    bool     little_endian_ = true;
    HeaderLayout header_;
    std::vector<MessageDef> messages_;
};

} // namespace flashtick
