#pragma once

// subplane app SDK: control channel client (see control/control_proto.h).
// Header-only, C++20, libc + std only. Never throws; every call returns bool
// and leaves the reason in last_error(). Cold path: blocking calls with a
// receive timeout (set_timeout_ms, default 5 s). Not thread-safe: use one
// ControlClient per thread or serialize calls.
//
// Late-join idiom (snapshot + live tail without missing or double-applying):
//
//   ControlClient ctl;
//   HelloInfo hi; AttachInfo ai;
//   if (!ctl.connect("/run/subplane/agent1.sock") || !ctl.hello(hi)) die(ctl.last_error());
//   SessionRequest req; req.name = "my_app"; req.msg_type_mask = 1u << int(MsgType::TRADE);
//   if (!ctl.create_session(req, ai)) die(ctl.last_error());   // or ctl.attach("name", ai)
//   ShmReader rd;
//   if (!rd.open(ai.ring_path) || rd.epoch() != hi.epoch) die("ring/epoch");
//   const uint64_t mark = rd.head();                // 1. mark the live position
//   std::unordered_map<uint32_t, uint64_t> last_seq; // per symbol
//   uint64_t snap_head = 0;
//   ctl.snapshot(req.name, snap_head, [&](const TickHeader& h, const uint8_t* p, uint32_t n) {
//       apply(h, p, n);                              // 2. seed state from the LVC
//       last_seq[h.symbol_id] = h.sequence;
//   });
//   rd.seek(mark);                                   // 3. replay everything since the mark
//   RecordView rv;
//   for (;;) {                                       // 4. tail; dedup the overlap by sequence
//       if (rd.poll(rv) != ReaderStatus::Ok) continue;
//       if (is_data(*rv.hdr)) {
//           uint64_t& s = last_seq[rv.hdr->symbol_id];
//           if (rv.hdr->sequence != 0 && rv.hdr->sequence <= s) continue;   // already in snapshot
//           s = rv.hdr->sequence;
//           apply(*rv.hdr, rv.payload, rv.payload_len);
//           if (!rv.valid()) resnapshot();
//       }
//   }

#include "subplane/client/shm_reader.h"
#include "subplane/control/control_proto.h"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

namespace subplane {

struct PartitionInfo {
    uint16_t    id = 0;
    std::string name;
};

struct HelloInfo {
    std::string agent_name;
    uint64_t    epoch = 0;
    uint32_t    record_version = 0;
    uint32_t    proto_version = 0;
    uint32_t    symbol_count = 0;
    uint32_t    max_symbols = 0;
    uint16_t    schema_id = 0;
    uint16_t    schema_version = 0;
    std::vector<PartitionInfo> partitions;
};

struct AttachInfo {
    uint32_t    id = 0;
    std::string name;
    std::string owner;
    std::string ring_path;
    uint64_t    epoch = 0;
    uint64_t    capacity = 0;
    uint32_t    max_payload = 0;
    uint32_t    filter_generation = 0;
};

struct SessionRequest {
    std::string name;
    uint64_t    ring_capacity_bytes = 64ull << 20;
    uint32_t    max_payload = 4096;
    uint64_t    msg_type_mask = 0;   // bit i = MsgType i; 0 = all data types
    uint64_t    partition_mask = 0;  // bit i = partition id i; 0 = all
    bool        all_symbols = true;
    bool        bus_admin = true;
    bool        agent_control = true;
    std::vector<std::string> symbols; // names or decimal instrument ids (<= 24 chars)
};

class ControlClient {
public:
    using SnapshotFn = std::function<void(const TickHeader&, const uint8_t*, uint32_t)>;

    ControlClient() = default;
    ControlClient(const ControlClient&) = delete;
    ControlClient& operator=(const ControlClient&) = delete;
    ~ControlClient() { close(); }

    bool connect(std::string_view socket_path) {
        close();
        sockaddr_un sa {};
        sa.sun_family = AF_UNIX;
        if (socket_path.empty() || socket_path.size() >= sizeof(sa.sun_path)) return fail("socket path empty or too long");
        std::memcpy(sa.sun_path, socket_path.data(), socket_path.size());
        const int fd = ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
        if (fd < 0) return fail_errno("socket");
        if (::connect(fd, reinterpret_cast<const sockaddr*>(&sa), sizeof(sa)) != 0) {
            const int e = errno;
            ::close(fd);
            errno = e;
            return fail_errno("connect " + std::string(socket_path));
        }
        fd_ = fd;
        rx_.assign(CTL_MAX_DATAGRAM, 0);
        return true;
    }

    void close() noexcept {
        if (fd_ >= 0) ::close(fd_);
        fd_ = -1;
    }

    bool is_connected() const noexcept { return fd_ >= 0; }
    const std::string& last_error() const noexcept { return err_; }
    // Status of the last failed reply (OK if the failure was local / transport).
    CtlStatus last_status() const noexcept { return last_status_; }
    void set_timeout_ms(int ms) noexcept { timeout_ms_ = ms; }

    bool hello(HelloInfo& out) {
        std::vector<uint8_t> body;
        if (!call(CtlOp::HELLO, nullptr, 0, body)) return false;
        CtlHelloResp r {};
        if (body.size() < sizeof(r)) return fail("HELLO: short reply");
        std::memcpy(&r, body.data(), sizeof(r));
        if (body.size() != sizeof(r) + static_cast<size_t>(r.partition_count) * sizeof(CtlPartitionEntry))
            return fail("HELLO: reply size does not match partition_count");
        out = HelloInfo {};
        out.agent_name = std::string(get_name(r.agent_name));
        out.epoch = r.epoch;
        out.record_version = r.record_version;
        out.proto_version = r.proto_version;
        out.symbol_count = r.symbol_count;
        out.max_symbols = r.max_symbols;
        out.schema_id = r.schema_id;
        out.schema_version = r.schema_version;
        for (uint16_t i = 0; i < r.partition_count; ++i) {
            CtlPartitionEntry e {};
            std::memcpy(&e, body.data() + sizeof(r) + i * sizeof(e), sizeof(e));
            out.partitions.push_back({e.id, std::string(get_name(e.name))});
        }
        return true;
    }

    bool attach(std::string_view name, AttachInfo& out) {
        CtlNameReq q {};
        if (!put_session_name(q.name, name)) return false;
        std::vector<uint8_t> body;
        if (!call(CtlOp::ATTACH, &q, sizeof(q), body)) return false;
        return parse_attach(body, out);
    }

    bool create_session(const SessionRequest& req, AttachInfo& out) {
        std::vector<uint8_t> q;
        if (!encode_session(req, q)) return false;
        std::vector<uint8_t> body;
        if (!call(CtlOp::CREATE_SESSION, q.data(), q.size(), body)) return false;
        return parse_attach(body, out);
    }

    bool set_filter(const SessionRequest& req, uint32_t& generation) {
        std::vector<uint8_t> q;
        if (!encode_session(req, q)) return false;
        std::vector<uint8_t> body;
        if (!call(CtlOp::SET_FILTER, q.data(), q.size(), body)) return false;
        CtlSetFilterResp r {};
        if (body.size() != sizeof(r)) return fail("SET_FILTER: bad reply size");
        std::memcpy(&r, body.data(), sizeof(r));
        generation = r.filter_generation;
        return true;
    }

    bool destroy_session(std::string_view name) {
        CtlNameReq q {};
        if (!put_session_name(q.name, name)) return false;
        std::vector<uint8_t> body;
        return call(CtlOp::DESTROY_SESSION, &q, sizeof(q), body);
    }

    // Calls fn once per LVC record (headers carry FLAG_SNAPSHOT_ORIGIN).
    // ring_head is the session ring's head observed before the snapshot.
    bool snapshot(std::string_view name, uint64_t& ring_head, const SnapshotFn& fn) {
        CtlNameReq q {};
        if (!put_session_name(q.name, name)) return false;
        std::vector<uint8_t> body;
        const uint32_t id = next_id_;
        if (!call(CtlOp::SNAPSHOT, &q, sizeof(q), body)) return false;
        CtlSnapshotResp r {};
        if (body.size() != sizeof(r)) return fail("SNAPSHOT: bad reply size");
        std::memcpy(&r, body.data(), sizeof(r));
        ring_head = r.ring_head_at_snapshot;
        for (uint32_t got = 0;; ) {
            CtlHeader h {};
            if (!recv_reply(CtlOp::SNAPSHOT, id, h, body)) return false;
            if (h.len == sizeof(CtlSnapshotEnd)) {
                CtlSnapshotEnd end {};
                std::memcpy(&end, body.data(), sizeof(end));
                if (end.done != 1 || end.record_count != got || got != r.record_count)
                    return fail("SNAPSHOT: record count mismatch");
                return true;
            }
            if (body.size() < sizeof(TickHeader)) return fail("SNAPSHOT: short record");
            // INTENT: copy the header out so it is properly aligned (the
            // receive buffer offset after CtlHeader is only 16-byte aligned).
            TickHeader th {};
            std::memcpy(&th, body.data(), sizeof(th));
            const auto plen = static_cast<uint32_t>(body.size() - sizeof(TickHeader));
            if (fn) fn(th, body.data() + sizeof(TickHeader), plen);
            ++got;
        }
    }

    // Full id -> name table, paged.
    bool symbols(std::vector<std::pair<uint32_t, std::string>>& out) {
        out.clear();
        uint32_t from = 1;
        for (;;) {
            CtlSymbolsReq q {from, CTL_SYMBOLS_PAGE_MAX};
            std::vector<uint8_t> body;
            if (!call(CtlOp::SYMBOLS, &q, sizeof(q), body)) return false;
            CtlSymbolsResp r {};
            if (body.size() < sizeof(r)) return fail("SYMBOLS: short reply");
            std::memcpy(&r, body.data(), sizeof(r));
            if (body.size() != sizeof(r) + static_cast<size_t>(r.count) * sizeof(CtlSymbolEntry))
                return fail("SYMBOLS: reply size does not match count");
            if (r.count == 0) return true;
            for (uint32_t i = 0; i < r.count; ++i) {
                CtlSymbolEntry e {};
                std::memcpy(&e, body.data() + sizeof(r) + static_cast<size_t>(i) * sizeof(e), sizeof(e));
                const size_t n = e.len <= CTL_SYMBOL_LEN ? e.len : CTL_SYMBOL_LEN;
                out.emplace_back(e.id, std::string(e.name, n));
            }
            if (r.next_id <= from) return fail("SYMBOLS: server did not advance");
            from = r.next_id;
        }
    }

    bool ping(uint64_t& rtt_ns) {
        CtlPingReq q {mono_ns()};
        std::vector<uint8_t> body;
        if (!call(CtlOp::PING, &q, sizeof(q), body)) return false;
        CtlPingResp r {};
        if (body.size() != sizeof(r)) return fail("PING: bad reply size");
        std::memcpy(&r, body.data(), sizeof(r));
        if (r.client_ns != q.client_ns) return fail("PING: echoed timestamp mismatch");
        rtt_ns = mono_ns() - q.client_ns;
        return true;
    }

    // Low-level: sends one raw datagram (tests, tooling). Returns false on send failure.
    bool send_raw(const void* data, size_t len) {
        if (fd_ < 0) return fail("not connected");
        const ssize_t n = ::send(fd_, data, len, MSG_NOSIGNAL);
        if (n < 0) return fail_errno("send");
        if (static_cast<size_t>(n) != len) return fail("send: short write");
        return true;
    }
    // Low-level: receives one reply datagram for (op, req_id); body is the error text on failure.
    bool recv_reply(CtlOp op, uint32_t req_id, CtlHeader& h, std::vector<uint8_t>& body) {
        if (fd_ < 0) return fail("not connected");
        for (;;) {
            pollfd p {fd_, POLLIN, 0};
            const int rc = ::poll(&p, 1, timeout_ms_);
            if (rc < 0) {
                if (errno == EINTR) continue;
                return fail_errno("poll");
            }
            if (rc == 0) return fail("timed out waiting for reply");
            break;
        }
        iovec iov {rx_.data(), rx_.size()};
        msghdr mh {};
        mh.msg_iov = &iov;
        mh.msg_iovlen = 1;
        ssize_t n;
        do { n = ::recvmsg(fd_, &mh, 0); } while (n < 0 && errno == EINTR);
        if (n < 0) return fail_errno("recv");
        if (n == 0) { close(); return fail("server closed the connection"); }
        if (mh.msg_flags & MSG_TRUNC) return fail("reply larger than CTL_MAX_DATAGRAM");
        const auto got = static_cast<size_t>(n);
        if (got < sizeof(CtlHeader)) return fail("reply shorter than CtlHeader");
        std::memcpy(&h, rx_.data(), sizeof(h));
        if (h.len != got - sizeof(CtlHeader)) return fail("reply len mismatch");
        if (h.req_id != req_id || h.op != static_cast<uint16_t>(op))
            return fail("reply does not match request (req_id " + std::to_string(h.req_id) + ")");
        body.assign(rx_.data() + sizeof(CtlHeader), rx_.data() + got);
        if (h.status != 0) {
            last_status_ = static_cast<CtlStatus>(h.status);
            err_ = std::string(ctl_status_name(h.status)) + ": " + std::string(body.begin(), body.end());
            return false;
        }
        return true;
    }
    uint32_t next_req_id() const noexcept { return next_id_; }

private:
    bool call(CtlOp op, const void* req, size_t len, std::vector<uint8_t>& body) {
        if (fd_ < 0) return fail("not connected");
        if (sizeof(CtlHeader) + len > CTL_MAX_DATAGRAM) return fail("request too large");
        last_status_ = CtlStatus::OK;
        const uint32_t id = next_id_++;
        tx_.resize(sizeof(CtlHeader) + len);
        CtlHeader h {};
        h.len = static_cast<uint32_t>(len);
        h.op = static_cast<uint16_t>(op);
        h.version = CONTROL_PROTO_VERSION;
        h.req_id = id;
        std::memcpy(tx_.data(), &h, sizeof(h));
        if (len != 0) std::memcpy(tx_.data() + sizeof(h), req, len);
        if (!send_raw(tx_.data(), tx_.size())) return false;
        CtlHeader rh {};
        return recv_reply(op, id, rh, body);
    }

    bool encode_session(const SessionRequest& r, std::vector<uint8_t>& out) {
        CtlSessionReq q {};
        if (!put_session_name(q.name, r.name)) return false;
        q.ring_capacity_bytes = r.ring_capacity_bytes;
        q.max_payload = r.max_payload;
        q.msg_type_mask = r.msg_type_mask;
        q.partition_mask = r.partition_mask;
        q.all_symbols = r.all_symbols ? 1 : 0;
        q.bus_admin = r.bus_admin ? 1 : 0;
        q.agent_control = r.agent_control ? 1 : 0;
        q.symbol_count = static_cast<uint32_t>(r.symbols.size());
        out.assign(sizeof(q) + r.symbols.size() * sizeof(CtlSymbolName), 0);
        std::memcpy(out.data(), &q, sizeof(q));
        for (size_t i = 0; i < r.symbols.size(); ++i) {
            CtlSymbolName s {};
            if (r.symbols[i].empty() || !set_name(s.name, r.symbols[i])) return fail("symbol name empty or longer than 24: " + r.symbols[i]);
            std::memcpy(out.data() + sizeof(q) + i * sizeof(s), &s, sizeof(s));
        }
        return true;
    }

    bool parse_attach(const std::vector<uint8_t>& body, AttachInfo& out) {
        CtlAttachResp r {};
        if (body.size() != sizeof(r)) return fail("attach reply: bad size");
        std::memcpy(&r, body.data(), sizeof(r));
        out.id = r.id;
        out.name = std::string(get_name(r.name));
        out.owner = std::string(get_name(r.owner));
        out.ring_path = std::string(get_name(r.ring_path));
        out.epoch = r.epoch;
        out.capacity = r.capacity;
        out.max_payload = r.max_payload;
        out.filter_generation = r.filter_generation;
        return true;
    }

    bool put_session_name(char (&dst)[CTL_NAME_LEN], std::string_view name) {
        if (name.empty() || name.size() >= CTL_NAME_LEN) return fail("session name must be 1..31 chars");
        set_name(dst, name);
        return true;
    }

    static uint64_t mono_ns() noexcept {
        timespec ts;
        ::clock_gettime(CLOCK_MONOTONIC, &ts);
        return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ull + static_cast<uint64_t>(ts.tv_nsec);
    }

    bool fail(std::string msg) {
        err_ = std::move(msg);
        return false;
    }
    bool fail_errno(const std::string& what) {
        const int e = errno;
        err_ = what + ": " + std::strerror(e);
        return false;
    }

    int                  fd_ = -1;
    uint32_t             next_id_ = 1;
    int                  timeout_ms_ = 5000;
    CtlStatus            last_status_ = CtlStatus::OK;
    std::string          err_;
    std::vector<uint8_t> rx_;
    std::vector<uint8_t> tx_;
};

} // namespace subplane
