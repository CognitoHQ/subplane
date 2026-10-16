#pragma once

// subplane control channel wire protocol. Shared by the agent's
// ControlServer and the header-only ControlClient (client/control_client.h).
// libc + C++20 std only: this file is part of the app-facing ABI.
//
// Transport: AF_UNIX SOCK_SEQPACKET. Every message (request or reply) is ONE
// datagram: CtlHeader (16 B) followed by exactly `len` body bytes. All
// integers are little-endian; all structs are packed. A datagram larger than
// CTL_MAX_DATAGRAM is rejected with TOO_LARGE.
//
// Request/reply: the client sends one request with a fresh req_id; the server
// answers with a reply carrying the same op and req_id. status == OK means the
// body is the op's response struct; status != OK means the body is a UTF-8
// error message (not NUL-terminated).
//
// Names (sessions, partitions, agent) are NUL-padded char[32], at most 31
// chars, [A-Za-z0-9_-] for session names. Symbol names are char[24], NUL-padded
// unless exactly 24 chars long.
//
// Op bodies (request -> reply):
//   HELLO            (empty)            -> CtlHelloResp + partition_count * CtlPartitionEntry
//   ATTACH           CtlNameReq         -> CtlAttachResp
//   CREATE_SESSION   CtlSessionReq + symbol_count * CtlSymbolName -> CtlAttachResp
//   SET_FILTER       CtlSessionReq + symbols (ring fields ignored) -> CtlSetFilterResp
//   DESTROY_SESSION  CtlNameReq         -> (empty)
//   PING             CtlPingReq         -> CtlPingResp
//   SYMBOLS          CtlSymbolsReq      -> CtlSymbolsResp + count * CtlSymbolEntry
//   SNAPSHOT         CtlNameReq         -> multi-datagram, see below
//   IDENTIFY         CtlIdentifyReq     -> (empty). Optional: names this
//                    connection's app for /topology. An older agent answers
//                    UNSUPPORTED, which a client treats as success.
//
// SNAPSHOT framing (all datagrams have op SNAPSHOT and the request's req_id):
//   1. status OK, body = CtlSnapshotResp { ring_head_at_snapshot, record_count, skipped }
//   2. record_count datagrams, status OK, body = TickHeader (64 B) + payload
//      (len = 64 + payload_len, so len >= 64). Headers carry FLAG_SNAPSHOT_ORIGIN.
//   3. one datagram, status OK, body = CtlSnapshotEnd { record_count, done = 1 }
//      (len = 8, which is how it is told apart from a record).
//   On error (e.g. NOT_FOUND) only a single error datagram is sent.
//   Records whose datagram would exceed CTL_MAX_DATAGRAM are not sent and are
//   counted in CtlSnapshotResp::skipped.
//   Late-join: seek the ring reader to ring_head_at_snapshot after applying
//   the snapshot and dedup the overlap by TickHeader::sequence.

#include "subplane/core/message.h"

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <type_traits>

namespace subplane {

static_assert(std::endian::native == std::endian::little, "control protocol assumes a little-endian host");

inline constexpr uint16_t CONTROL_PROTO_VERSION = 1;
inline constexpr uint32_t CTL_MAX_DATAGRAM      = 64 * 1024;  // header + body
inline constexpr uint32_t CTL_NAME_LEN          = 32;
inline constexpr uint32_t CTL_SYMBOL_LEN        = 24;
inline constexpr uint32_t CTL_SYMBOLS_PAGE_MAX  = 1024;       // entries per SYMBOLS reply

enum class CtlOp : uint16_t {
    HELLO           = 1,
    ATTACH          = 2,
    CREATE_SESSION  = 3,
    SET_FILTER      = 4,
    DESTROY_SESSION = 5,
    SNAPSHOT        = 6,
    PING            = 7,
    SYMBOLS         = 8,
    IDENTIFY        = 9,
};

enum class CtlStatus : uint32_t {
    OK          = 0,
    BAD_REQUEST = 1,
    NOT_FOUND   = 2,
    EXISTS      = 3,
    INTERNAL    = 4,
    UNSUPPORTED = 5,
    TOO_LARGE   = 6,
};

inline const char* ctl_status_name(uint32_t s) noexcept {
    switch (static_cast<CtlStatus>(s)) {
    case CtlStatus::OK:          return "OK";
    case CtlStatus::BAD_REQUEST: return "BAD_REQUEST";
    case CtlStatus::NOT_FOUND:   return "NOT_FOUND";
    case CtlStatus::EXISTS:      return "EXISTS";
    case CtlStatus::INTERNAL:    return "INTERNAL";
    case CtlStatus::UNSUPPORTED: return "UNSUPPORTED";
    case CtlStatus::TOO_LARGE:   return "TOO_LARGE";
    }
    return "UNKNOWN_STATUS";
}

#pragma pack(push, 1)

struct CtlHeader {
    uint32_t len;      // bytes of body following this header
    uint16_t op;       // CtlOp
    uint16_t version;  // CONTROL_PROTO_VERSION
    uint32_t req_id;   // echoed in every reply datagram
    uint32_t status;   // 0 ok, else CtlStatus
};

// HELLO reply.
struct CtlHelloResp {
    char     agent_name[32];
    uint64_t epoch;
    uint32_t record_version;
    uint32_t proto_version;
    uint32_t symbol_count;
    uint32_t max_symbols;
    uint16_t schema_id;       // of the first partition (0 if none)
    uint16_t schema_version;
    uint16_t partition_count; // CtlPartitionEntry records follow
    uint16_t pad;
};
struct CtlPartitionEntry {
    uint16_t id;
    char     name[32];
};

// ATTACH / DESTROY_SESSION / SNAPSHOT request.
struct CtlNameReq {
    char name[32];
};

// ATTACH / CREATE_SESSION reply.
struct CtlAttachResp {
    uint32_t id;
    char     name[32];
    char     owner[8];        // "config" | "client"
    char     ring_path[224];
    uint64_t epoch;
    uint64_t capacity;
    uint32_t max_payload;
    uint32_t filter_generation;
};

// CREATE_SESSION / SET_FILTER request; followed by symbol_count CtlSymbolName.
struct CtlSessionReq {
    char     name[32];
    uint64_t ring_capacity_bytes; // power of two >= 64 KiB (ignored by SET_FILTER)
    uint32_t max_payload;         // 1..capacity/4 (ignored by SET_FILTER)
    uint64_t msg_type_mask;       // bit i = data msg_type i (MsgType); 0 = all data types
    uint64_t partition_mask;      // bit i = partition id i; 0 = all
    uint8_t  all_symbols;
    uint8_t  bus_admin;
    uint8_t  agent_control;
    uint8_t  recap;               // 1 = ask for recap (CREATE_SESSION); was padding, old clients send 0
    uint32_t symbol_count;
};
struct CtlSymbolName {
    char name[24];
};

struct CtlSetFilterResp {
    uint32_t filter_generation;
};

struct CtlSnapshotResp {
    uint64_t ring_head_at_snapshot;
    uint32_t record_count;        // record datagrams that follow
    uint32_t skipped;             // records too large for one datagram (not sent)
};
struct CtlSnapshotEnd {
    uint32_t record_count;
    uint32_t done;                // always 1
};

struct CtlSymbolsReq {
    uint32_t from_id;             // first id to return (ids start at 1)
    uint32_t max;                 // capped at CTL_SYMBOLS_PAGE_MAX
};
struct CtlSymbolsResp {
    uint32_t count;               // CtlSymbolEntry records follow; 0 = no more ids
    uint32_t next_id;             // pass as from_id for the next page
};
struct CtlSymbolEntry {
    uint32_t id;
    uint8_t  len;
    char     name[24];
    uint8_t  pad[3];
};

struct CtlPingReq {
    uint64_t client_ns;
};
struct CtlPingResp {
    uint64_t client_ns;
    uint64_t server_ns;
};

// IDENTIFY: printable ASCII, 1 to 31 chars, NUL-padded.
struct CtlIdentifyReq {
    char app_name[32];
};

#pragma pack(pop)

static_assert(sizeof(CtlIdentifyReq) == 32);
static_assert(sizeof(CtlHeader) == 16);
static_assert(sizeof(CtlHelloResp) == 64);
static_assert(sizeof(CtlPartitionEntry) == 34);
static_assert(sizeof(CtlNameReq) == 32);
static_assert(sizeof(CtlAttachResp) == 292);
static_assert(sizeof(CtlSessionReq) == 68);
static_assert(sizeof(CtlSymbolName) == 24);
static_assert(sizeof(CtlSetFilterResp) == 4);
static_assert(sizeof(CtlSnapshotResp) == 16);
static_assert(sizeof(CtlSnapshotEnd) == 8);
static_assert(sizeof(CtlSymbolsReq) == 8);
static_assert(sizeof(CtlSymbolsResp) == 8);
static_assert(sizeof(CtlSymbolEntry) == 32);
static_assert(sizeof(CtlPingReq) == 8);
static_assert(sizeof(CtlPingResp) == 16);
static_assert(std::is_trivially_copyable_v<CtlSessionReq> && std::is_trivially_copyable_v<CtlAttachResp>);
// A snapshot record datagram must be distinguishable from CtlSnapshotEnd by length.
static_assert(sizeof(TickHeader) > sizeof(CtlSnapshotEnd));

// Copies `s` into a fixed NUL-padded field. Returns false (and truncates) if
// `s` does not fit in N bytes. A string of exactly N bytes is stored without
// a terminator; use get_name() to read fields back.
template <size_t N>
inline bool set_name(char (&dst)[N], std::string_view s) noexcept {
    std::memset(dst, 0, N);
    const size_t n = s.size() < N ? s.size() : N;
    std::memcpy(dst, s.data(), n);
    return s.size() <= N;
}

template <size_t N>
inline std::string_view get_name(const char (&src)[N]) noexcept {
    size_t n = 0;
    while (n < N && src[n] != '\0') ++n;
    return {src, n};
}

} // namespace subplane
