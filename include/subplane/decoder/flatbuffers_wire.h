#pragma once

// Minimal FlatBuffers wire reader for routing. No FlatBuffers library: the
// agent reads routing fields straight off the published wire format and never
// decodes the body.
//
// INTENT: the wire layout (flatbuffers "Internals" doc, stable since 2014):
//   buf[0..4)       uoffset to the root table, relative to its own position
//   table[0..4)     soffset: vtable = table - soffset (signed, may be negative)
//   vtable[0..2)    vtable size in bytes;  vtable[2..4) inline table size
//   vtable[4+2*id]  u16 offset of field `id` from the table start; 0 = absent
//   table/string/union-value fields hold a uoffset relative to the field
//   strings: u32 length then bytes (then a NUL not counted)
// A field whose slot lies beyond the vtable size was added after the producer's
// schema version: absent, default applies. Every load is bounds-checked against
// the buffer because the bytes come off the wire unverified; a malformed buffer
// must yield "absent"/false, never a read outside [b, b+n). Little-endian host.

#include <cstdint>
#include <cstring>

namespace subplane::fbw {

struct Buf {
    const uint8_t* b;
    uint32_t       n;
};

template <class T>
inline bool rd(Buf s, uint32_t at, T& v) noexcept {
    if (at > s.n || s.n - at < sizeof(T)) return false;
    std::memcpy(&v, s.b + at, sizeof(T));
    return true;
}

// Root table position. `prefix` is 4 for a size-prefixed buffer, else 0.
inline bool root(Buf s, uint32_t prefix, uint32_t& tbl) noexcept {
    uint32_t off;
    if (!rd(s, prefix, off)) return false;
    if (off > s.n - prefix) return false;
    tbl = prefix + off;
    return tbl + 4 <= s.n;
}

// Byte position of field `id` in the table at `tbl`; 0 if absent.
inline uint32_t field(Buf s, uint32_t tbl, uint16_t id) noexcept {
    int32_t so;
    if (!rd(s, tbl, so)) return 0;
    const int64_t vt = static_cast<int64_t>(tbl) - so;
    if (vt < 0 || vt + 4 > static_cast<int64_t>(s.n)) return 0;
    uint16_t vsize;
    if (!rd(s, static_cast<uint32_t>(vt), vsize)) return 0;
    const uint32_t slot = 4u + 2u * id;
    if (slot + 2 > vsize) return 0; // field newer than the producer's schema
    uint16_t fo;
    if (!rd(s, static_cast<uint32_t>(vt) + slot, fo)) return 0;
    if (fo == 0) return 0;
    const uint32_t at = tbl + fo;
    return at < s.n ? at : 0;
}

template <class T>
inline T scalar(Buf s, uint32_t tbl, uint16_t id, T def) noexcept {
    const uint32_t at = field(s, tbl, id);
    T v;
    return (at != 0 && rd(s, at, v)) ? v : def;
}

// Follows a table, string or union-value field: target position, 0 if absent.
inline uint32_t deref(Buf s, uint32_t tbl, uint16_t id) noexcept {
    const uint32_t at = field(s, tbl, id);
    uint32_t off;
    if (at == 0 || !rd(s, at, off)) return 0;
    if (off > s.n - at) return 0;
    const uint32_t t = at + off;
    return (t != 0 && t + 4 <= s.n) ? t : 0;
}

inline bool string(Buf s, uint32_t tbl, uint16_t id, const char*& p, uint32_t& len) noexcept {
    const uint32_t at = deref(s, tbl, id);
    uint32_t n;
    if (at == 0 || !rd(s, at, n)) return false;
    if (n > s.n - at - 4) return false;
    p = reinterpret_cast<const char*>(s.b + at + 4);
    len = n;
    return true;
}

// The 4-byte file identifier that follows the root offset, when the schema declares one.
inline bool file_id(Buf s, uint32_t prefix, char out[4]) noexcept {
    if (s.n < prefix + 8) return false;
    std::memcpy(out, s.b + prefix + 4, 4);
    return true;
}

} // namespace subplane::fbw
