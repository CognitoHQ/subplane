#include "subplane/control/control_server.h"
#include "subplane/pipeline/topic_registry.h"

#include "subplane/core/clock.h"
#include "subplane/logging/log_registry.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstring>

#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

namespace subplane {

namespace {

constexpr uint64_t kServiceIntervalNs = 100'000'000ull;  // service_linger cadence
constexpr int      kSendTimeoutMs = 2000;                // a client that does not drain replies is dropped
constexpr int      kMaxDgramsPerWakeup = 16;             // fairness between clients

std::string errno_str(int e) { return std::string(std::strerror(e)); }

bool valid_session_name(std::string_view n) {
    if (n.empty() || n.size() > CTL_NAME_LEN - 1) return false;
    for (char ch : n) {
        const auto u = static_cast<unsigned char>(ch);
        if (!(std::isalnum(u) || ch == '_' || ch == '-')) return false;
    }
    return true;
}

bool fill_sockaddr(const std::string& path, sockaddr_un& sa) {
    std::memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_UNIX;
    if (path.empty() || path.size() >= sizeof(sa.sun_path)) return false;
    std::memcpy(sa.sun_path, path.c_str(), path.size());
    return true;
}

} // namespace

ControlServer::ControlServer(Config cfg, SessionManager& sessions, SymbolTable& symbols,
                             const std::vector<Partition*>& partitions, std::string agent_name, uint64_t epoch)
    : cfg_(std::move(cfg)), sessions_(sessions), symbols_(symbols), partitions_(partitions),
      agent_name_(std::move(agent_name)), epoch_(epoch) {}

ControlServer::~ControlServer() { stop(); }

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

bool ControlServer::bind_at(const std::string& path, std::string& err) {
    sockaddr_un sa;
    if (!fill_sockaddr(path, sa)) {
        err = "control socket path empty or too long: " + path;
        errno = ENAMETOOLONG;
        return false;
    }
    // Stale socket from a previous run: only remove it if it is a socket and
    // nobody is accepting on it (never unlink a live agent's socket or a
    // regular file that happens to have the name).
    struct stat st {};
    if (::lstat(path.c_str(), &st) == 0) {
        if (!S_ISSOCK(st.st_mode)) {
            err = "control socket path exists and is not a socket: " + path;
            errno = EEXIST;
            return false;
        }
        const int probe = ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
        if (probe >= 0) {
            const bool live = ::connect(probe, reinterpret_cast<const sockaddr*>(&sa), sizeof(sa)) == 0;
            ::close(probe);
            if (live) {
                err = "control socket in use by another process: " + path;
                errno = EADDRINUSE;
                return false;
            }
        }
        ::unlink(path.c_str());
    }

    const int fd = ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        const int e = errno;
        err = "socket(AF_UNIX, SOCK_SEQPACKET): " + errno_str(e);
        errno = e;
        return false;
    }
    if (::bind(fd, reinterpret_cast<const sockaddr*>(&sa), sizeof(sa)) != 0) {
        const int e = errno;
        err = "bind " + path + ": " + errno_str(e);
        ::close(fd);
        errno = e;
        return false;
    }
    if (::listen(fd, 64) != 0) {
        const int e = errno;
        err = "listen " + path + ": " + errno_str(e);
        ::close(fd);
        ::unlink(path.c_str());
        errno = e;
        return false;
    }
    listen_fd_ = fd;
    bound_path_ = path;
    return true;
}

bool ControlServer::start(std::string& err) {
    if (listen_fd_ >= 0) {
        err = "control server already started";
        return false;
    }
    if (!bind_at(cfg_.socket_path, err)) {
        const int e = errno;
        const bool try_fallback = (e == EACCES || e == ENOENT || e == EPERM) && !cfg_.fallback_socket_path.empty() &&
                                  cfg_.fallback_socket_path != cfg_.socket_path;
        if (!try_fallback) return false;
        std::string err2;
        if (!bind_at(cfg_.fallback_socket_path, err2)) {
            err += "; fallback: " + err2;
            return false;
        }
        LogRegistry::instance().warn("CONTROL", "FALLBACK_SOCKET",
                                     err + "; using " + cfg_.fallback_socket_path);
        err.clear();
    }
    wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd_ < 0) {
        err = "eventfd: " + errno_str(errno);
        ::close(listen_fd_);
        listen_fd_ = -1;
        ::unlink(bound_path_.c_str());
        return false;
    }
    rx_.assign(CTL_MAX_DATAGRAM, 0);
    stop_.store(false, std::memory_order_relaxed);
    thread_ = std::thread([this] { run(); });
    LogRegistry::instance().info("CONTROL", "LISTENING",
                                 bound_path_ + " (linger_ms=" + std::to_string(cfg_.linger_ns / 1'000'000) +
                                     ", max_clients=" + std::to_string(cfg_.max_clients) + ")");
    return true;
}

void ControlServer::stop() {
    if (thread_.joinable()) {
        stop_.store(true, std::memory_order_release);
        const uint64_t one = 1;
        [[maybe_unused]] const ssize_t w = ::write(wake_fd_, &one, sizeof(one));
        thread_.join();
    }
    // Thread is gone: clients_ is ours. Closing a connection counts as a
    // client disconnect so lingering semantics stay the same on shutdown.
    while (!clients_.empty()) drop(clients_.size() - 1);
    publish_clients();
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        ::unlink(bound_path_.c_str());
        LogRegistry::instance().info("CONTROL", "STOPPED", bound_path_);
    }
    if (wake_fd_ >= 0) {
        ::close(wake_fd_);
        wake_fd_ = -1;
    }
}

// ---------------------------------------------------------------------------
// Poll loop
// ---------------------------------------------------------------------------

void ControlServer::run() {
    uint64_t next_service = now_mono_ns() + kServiceIntervalNs;
    std::vector<pollfd> pfds;
    while (!stop_.load(std::memory_order_acquire)) {
        pfds.clear();
        pfds.push_back({listen_fd_, POLLIN, 0});
        pfds.push_back({wake_fd_, POLLIN, 0});
        for (const auto& c : clients_) pfds.push_back({c.fd, POLLIN, 0});

        const uint64_t now = now_mono_ns();
        const int timeout_ms = now >= next_service ? 0 : static_cast<int>((next_service - now) / 1'000'000ull) + 1;
        const int rc = ::poll(pfds.data(), static_cast<nfds_t>(pfds.size()), timeout_ms);
        if (rc < 0 && errno != EINTR) {
            LogRegistry::instance().error("CONTROL", "POLL_FAILED", errno_str(errno));
            break;
        }

        if (now_mono_ns() >= next_service) {
            // INTENT: SessionManager compares these against the timestamps we
            // pass to client_disconnected(); both use now_ns().
            sessions_.service_linger(now_ns());
            sessions_.service_readers(now_ns());
            next_service = now_mono_ns() + kServiceIntervalNs;
        }
        if (rc <= 0) continue;
        if (pfds[1].revents != 0) break; // stop() woke us

        // Clients first, highest index first, so drop() never shifts an index
        // we have yet to visit; accept() only appends afterwards.
        for (size_t i = clients_.size(); i-- > 0;) {
            const short re = pfds[i + 2].revents;
            if (re == 0) continue;
            bool keep = true;
            if (re & POLLIN) {
                for (int k = 0; k < kMaxDgramsPerWakeup; ++k) {
                    const Rx r = serve(clients_[i]);
                    if (r == Rx::DROP) keep = false;
                    if (r != Rx::SERVED) break;
                }
            } else if (re & (POLLHUP | POLLERR | POLLNVAL)) {
                keep = false;
            }
            if (!keep) drop(i);
        }
        if (pfds[0].revents & POLLIN) accept_clients();
        if (clients_dirty_) publish_clients();
    }
}

void ControlServer::note_attached(Client& c, const std::string& name) {
    if (std::find(c.attached.begin(), c.attached.end(), name) == c.attached.end()) c.attached.push_back(name);
    clients_dirty_ = true;
}

void ControlServer::publish_clients() {
    std::vector<ClientInfo> v;
    v.reserve(clients_.size());
    for (const auto& c : clients_) v.push_back({c.pid, c.uid, c.app_name, c.attached});
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    snapshot_ = std::move(v);
    clients_dirty_ = false;
}

std::vector<ControlServer::ClientInfo> ControlServer::client_snapshot() const {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    return snapshot_;
}

void ControlServer::accept_clients() {
    for (;;) {
        const int fd = ::accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EINTR) continue;
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                LogRegistry::instance().warn("CONTROL", "ACCEPT_FAILED", errno_str(errno));
            }
            return;
        }
        Client c;
        c.fd = fd;
        ucred cred {};
        socklen_t cl = sizeof(cred);
        if (::getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cred, &cl) == 0) {
            c.uid = cred.uid;
            c.pid = cred.pid;
        }
        const std::string who = "uid=" + std::to_string(c.uid) + " pid=" + std::to_string(c.pid);
        if (clients_.size() >= cfg_.max_clients) {
            LogRegistry::instance().warn("CONTROL", "CLIENT_REJECTED",
                                         who + ": max_clients " + std::to_string(cfg_.max_clients) + " reached");
            ::close(fd);
            continue;
        }
        clients_.push_back(std::move(c));
        n_clients_.store(clients_.size(), std::memory_order_relaxed);
        clients_dirty_ = true;
        LogRegistry::instance().info("CONTROL", "CLIENT_CONNECTED", who);
    }
}

bool ControlServer::held_by_other(const std::string& name, const Client* except) const {
    for (const auto& c : clients_) {
        if (&c == except) continue;
        if (std::find(c.holds.begin(), c.holds.end(), name) != c.holds.end()) return true;
    }
    return false;
}

void ControlServer::release_everywhere(const std::string& name) {
    for (auto& c : clients_) {
        c.holds.erase(std::remove(c.holds.begin(), c.holds.end(), name), c.holds.end());
        c.attached.erase(std::remove(c.attached.begin(), c.attached.end(), name), c.attached.end());
    }
    clients_dirty_ = true;
}

void ControlServer::drop(size_t idx) {
    Client& c = clients_[idx];
    const uint64_t now = now_ns();
    for (const auto& name : c.holds) {
        // INTENT: linger starts only when the LAST holder goes away; another
        // connection that attached to the same session keeps it alive.
        if (!held_by_other(name, &c)) sessions_.client_disconnected(name, now);
    }
    LogRegistry::instance().info("CONTROL", "CLIENT_DISCONNECTED",
                                 "uid=" + std::to_string(c.uid) + " pid=" + std::to_string(c.pid) +
                                     " sessions_released=" + std::to_string(c.holds.size()));
    ::close(c.fd);
    clients_.erase(clients_.begin() + static_cast<std::ptrdiff_t>(idx));
    n_clients_.store(clients_.size(), std::memory_order_relaxed);
    clients_dirty_ = true;
}

// ---------------------------------------------------------------------------
// I/O helpers
// ---------------------------------------------------------------------------

bool ControlServer::send_dgram(Client& c, const void* data, size_t len) {
    for (;;) {
        const ssize_t n = ::send(c.fd, data, len, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (n >= 0) return static_cast<size_t>(n) == len;
        if (errno == EINTR) continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK) return false;
        // Client's receive queue is full (e.g. mid-snapshot). Wait a bounded
        // time for it to drain; a client that stops reading is dropped.
        pollfd p {c.fd, POLLOUT, 0};
        const int rc = ::poll(&p, 1, kSendTimeoutMs);
        if (rc == 0) return false;
        if (rc < 0 && errno != EINTR) return false;
        if (rc > 0 && (p.revents & (POLLERR | POLLHUP | POLLNVAL))) return false;
    }
}

bool ControlServer::reply(Client& c, const CtlHeader& req, CtlStatus st, const void* body, size_t len) {
    std::vector<uint8_t> out(sizeof(CtlHeader) + len);
    CtlHeader h {};
    h.len = static_cast<uint32_t>(len);
    h.op = req.op;
    h.version = CONTROL_PROTO_VERSION;
    h.req_id = req.req_id;
    h.status = static_cast<uint32_t>(st);
    std::memcpy(out.data(), &h, sizeof(h));
    if (len != 0) std::memcpy(out.data() + sizeof(h), body, len);
    return send_dgram(c, out.data(), out.size());
}

bool ControlServer::reply_err(Client& c, const CtlHeader& req, CtlStatus st, const std::string& msg) {
    const size_t n = std::min<size_t>(msg.size(), 1024);
    return reply(c, req, st, msg.data(), n);
}

ControlServer::Rx ControlServer::serve(Client& c) {
    iovec iov {rx_.data(), rx_.size()};
    msghdr mh {};
    mh.msg_iov = &iov;
    mh.msg_iovlen = 1;
    const ssize_t n = ::recvmsg(c.fd, &mh, MSG_DONTWAIT);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return Rx::DRAINED;
        return Rx::DROP;
    }
    if (n == 0) return Rx::DROP; // orderly shutdown

    CtlHeader h {};
    const auto got = static_cast<size_t>(n);
    if (got >= sizeof(h)) std::memcpy(&h, rx_.data(), sizeof(h));
    if (mh.msg_flags & MSG_TRUNC) {
        return reply_err(c, h, CtlStatus::TOO_LARGE,
                         "request exceeds " + std::to_string(CTL_MAX_DATAGRAM) + " bytes")
                   ? Rx::SERVED : Rx::DROP;
    }
    if (got < sizeof(h)) {
        return reply_err(c, h, CtlStatus::BAD_REQUEST, "datagram shorter than CtlHeader") ? Rx::SERVED : Rx::DROP;
    }
    if (h.version != CONTROL_PROTO_VERSION) {
        return reply_err(c, h, CtlStatus::BAD_REQUEST,
                         "unsupported protocol version " + std::to_string(h.version) + " (server speaks " +
                             std::to_string(CONTROL_PROTO_VERSION) + ")")
                   ? Rx::SERVED : Rx::DROP;
    }
    if (h.len != got - sizeof(h)) {
        return reply_err(c, h, CtlStatus::BAD_REQUEST, "header len does not match datagram size") ? Rx::SERVED
                                                                                                 : Rx::DROP;
    }
    const uint8_t* body = rx_.data() + sizeof(h);
    bool ok = false;
    switch (static_cast<CtlOp>(h.op)) {
    case CtlOp::HELLO:           ok = op_hello(c, h); break;
    case CtlOp::ATTACH:          ok = op_attach(c, h, body); break;
    case CtlOp::CREATE_SESSION:  ok = op_create(c, h, body); break;
    case CtlOp::SET_FILTER:      ok = op_set_filter(c, h, body); break;
    case CtlOp::DESTROY_SESSION: ok = op_destroy(c, h, body); break;
    case CtlOp::SNAPSHOT:        ok = op_snapshot(c, h, body); break;
    case CtlOp::PING:            ok = op_ping(c, h, body); break;
    case CtlOp::SYMBOLS:         ok = op_symbols(c, h, body); break;
    case CtlOp::IDENTIFY:        ok = op_identify(c, h, body); break;
    default:
        ok = reply_err(c, h, CtlStatus::UNSUPPORTED, "unknown op " + std::to_string(h.op));
        break;
    }
    return ok ? Rx::SERVED : Rx::DROP;
}

// ---------------------------------------------------------------------------
// Request decoding
// ---------------------------------------------------------------------------

bool ControlServer::to_spec(const CtlHeader& h, const uint8_t* body, bool check_ring, SessionSpec& out,
                            std::string& err) const {
    if (h.len < sizeof(CtlSessionReq)) {
        err = "session request body too short";
        return false;
    }
    CtlSessionReq r {};
    std::memcpy(&r, body, sizeof(r));
    if (r.symbol_count > (h.len - sizeof(CtlSessionReq)) / sizeof(CtlSymbolName) ||
        h.len != sizeof(CtlSessionReq) + static_cast<size_t>(r.symbol_count) * sizeof(CtlSymbolName) +
                 static_cast<size_t>(r.topic_count) * sizeof(CtlTopicPattern)) {
        err = "symbol_count and topic_count do not match body length";
        return false;
    }
    out = SessionSpec {};
    out.name = std::string(get_name(r.name));
    if (!valid_session_name(out.name)) {
        err = "session name must be 1..31 chars of [A-Za-z0-9_-]";
        return false;
    }
    out.owner = "client";
    if (check_ring) {
        if (!is_pow2(r.ring_capacity_bytes) || r.ring_capacity_bytes < RING_MIN_CAPACITY ||
            r.ring_capacity_bytes > kMaxClientRingBytes) {
            err = "ring_capacity_bytes must be a power of two in [64 KiB, " +
                  std::to_string(kMaxClientRingBytes >> 20) + " MiB]";
            return false;
        }
        if (r.max_payload == 0 || r.max_payload > r.ring_capacity_bytes / 4) {
            err = "max_payload must be 1..capacity/4";
            return false;
        }
        out.ring_capacity_bytes = r.ring_capacity_bytes;
        out.max_payload = r.max_payload;
    }
    for (uint32_t bit = 0; bit < 64; ++bit) {
        if (!((r.msg_type_mask >> bit) & 1u)) continue;
        const char* name = msg_type_name(static_cast<uint8_t>(bit));
        uint8_t code = 0;
        if (bit == 0 || !msg_type_from_name(name, code)) {
            err = "msg_type_mask bit " + std::to_string(bit) + " is not a known data msg_type";
            return false;
        }
        out.msg_types.emplace_back(name);
    }
    out.all_symbols = r.all_symbols != 0;
    out.bus_admin = r.bus_admin != 0;
    out.agent_control = r.agent_control != 0;
    out.recap = r.recap != 0;
    const uint8_t* syms = body + sizeof(CtlSessionReq);
    for (uint32_t i = 0; i < r.symbol_count; ++i) {
        CtlSymbolName sn {};
        std::memcpy(&sn, syms + static_cast<size_t>(i) * sizeof(sn), sizeof(sn));
        const std::string_view s = get_name(sn.name);
        if (s.empty()) {
            err = "empty symbol name at index " + std::to_string(i);
            return false;
        }
        out.symbols.emplace_back(s);
    }
    const uint8_t* tops = syms + static_cast<size_t>(r.symbol_count) * sizeof(CtlSymbolName);
    for (uint32_t i = 0; i < r.topic_count; ++i) {
        CtlTopicPattern tp {};
        std::memcpy(&tp, tops + static_cast<size_t>(i) * sizeof(tp), sizeof(tp));
        std::string pat(get_name(tp.pattern));
        if (!valid_topic_pattern(pat, err)) return false;
        out.topics.push_back(std::move(pat));
    }
    return true;
}

bool ControlServer::fill_attach(const std::string& name, CtlAttachResp& out, std::string& err) const {
    SessionAttachInfo info;
    if (!sessions_.attach_info(name, info)) {
        err = "session " + name + " not found";
        return false;
    }
    if (info.ring_path.size() >= sizeof(out.ring_path)) {
        err = "ring path too long for the protocol: " + info.ring_path;
        return false;
    }
    out = CtlAttachResp {};
    out.id = info.id;
    set_name(out.name, info.name);
    set_name(out.owner, info.owner);
    set_name(out.ring_path, info.ring_path);
    out.epoch = info.epoch;
    out.capacity = info.capacity;
    out.max_payload = info.max_payload;
    out.filter_generation = info.filter_generation;
    return true;
}

namespace {
bool read_name_req(const CtlHeader& h, const uint8_t* body, std::string& name) {
    if (h.len != sizeof(CtlNameReq)) return false;
    CtlNameReq r {};
    std::memcpy(&r, body, sizeof(r));
    name = std::string(get_name(r.name));
    return !name.empty() && name.size() < CTL_NAME_LEN;
}
} // namespace

// ---------------------------------------------------------------------------
// Ops
// ---------------------------------------------------------------------------

bool ControlServer::op_hello(Client& c, const CtlHeader& h) {
    if (h.len != 0) return reply_err(c, h, CtlStatus::BAD_REQUEST, "HELLO takes no body");
    CtlHelloResp r {};
    set_name(r.agent_name, std::string_view(agent_name_).substr(0, CTL_NAME_LEN - 1));
    r.epoch = epoch_;
    r.record_version = RECORD_VERSION;
    r.proto_version = CONTROL_PROTO_VERSION;
    r.symbol_count = symbols_.size();
    r.max_symbols = symbols_.capacity();
    if (!partitions_.empty()) {
        if (const SbeSchema* s = partitions_.front()->schema()) {
            r.schema_id = s->schema_id();
            r.schema_version = s->version();
        }
    }
    std::vector<CtlTopicEntry> topics;
    for (const Partition* p : partitions_) {
        bool known = false;
        for (auto& t : topics) {
            if (t.topic_id == p->topic_id()) { ++t.partitions; known = true; break; }
        }
        if (known) continue;
        CtlTopicEntry e {};
        e.topic_id = p->topic_id();
        e.partitions = 1;
        set_name(e.name, p->topic_name());
        topics.push_back(e);
    }
    r.topic_count = static_cast<uint16_t>(topics.size());
    std::vector<uint8_t> body(sizeof(r) + topics.size() * sizeof(CtlTopicEntry));
    std::memcpy(body.data(), &r, sizeof(r));
    if (!topics.empty()) std::memcpy(body.data() + sizeof(r), topics.data(), topics.size() * sizeof(CtlTopicEntry));
    return reply(c, h, CtlStatus::OK, body.data(), body.size());
}

bool ControlServer::op_attach(Client& c, const CtlHeader& h, const uint8_t* body) {
    std::string name;
    if (!read_name_req(h, body, name)) return reply_err(c, h, CtlStatus::BAD_REQUEST, "ATTACH: bad name");
    CtlAttachResp r;
    std::string err;
    if (!fill_attach(name, r, err)) return reply_err(c, h, CtlStatus::NOT_FOUND, err);
    if (get_name(r.owner) == "client") {
        sessions_.client_reattached(name);
        if (std::find(c.holds.begin(), c.holds.end(), name) == c.holds.end()) c.holds.push_back(name);
    }
    note_attached(c, name);
    LogRegistry::instance().info("CONTROL", "ATTACH",
                                 name + " by uid=" + std::to_string(c.uid) + " pid=" + std::to_string(c.pid));
    return reply(c, h, CtlStatus::OK, &r, sizeof(r));
}

bool ControlServer::op_create(Client& c, const CtlHeader& h, const uint8_t* body) {
    SessionSpec spec;
    std::string err;
    if (!to_spec(h, body, true, spec, err)) return reply_err(c, h, CtlStatus::BAD_REQUEST, err);
    SessionAttachInfo existing;
    if (sessions_.attach_info(spec.name, existing)) {
        return reply_err(c, h, CtlStatus::EXISTS, "session " + spec.name + " already exists");
    }
    // Validate partitions/symbols/msg types up front so client mistakes are
    // BAD_REQUEST and only ring/pipeline failures are INTERNAL.
    FilterSpec fs;
    uint64_t mask = 0;
    Pipeline* group = nullptr;
    if (!sessions_.resolve(spec, fs, mask, group, err)) return reply_err(c, h, CtlStatus::BAD_REQUEST, err);
    if (!sessions_.create(spec, err)) {
        // A config reload may have created the same name in between.
        const bool exists = sessions_.attach_info(spec.name, existing);
        return reply_err(c, h, exists ? CtlStatus::EXISTS : CtlStatus::INTERNAL, err);
    }
    c.holds.push_back(spec.name);
    note_attached(c, spec.name);
    CtlAttachResp r;
    if (!fill_attach(spec.name, r, err)) return reply_err(c, h, CtlStatus::INTERNAL, err);
    LogRegistry::instance().info("CONTROL", "SESSION_CREATED",
                                 spec.name + " by uid=" + std::to_string(c.uid) + " pid=" + std::to_string(c.pid));
    return reply(c, h, CtlStatus::OK, &r, sizeof(r));
}

bool ControlServer::op_set_filter(Client& c, const CtlHeader& h, const uint8_t* body) {
    SessionSpec spec;
    std::string err;
    if (!to_spec(h, body, false, spec, err)) return reply_err(c, h, CtlStatus::BAD_REQUEST, err);
    SessionAttachInfo info;
    if (!sessions_.attach_info(spec.name, info)) {
        return reply_err(c, h, CtlStatus::NOT_FOUND, "session " + spec.name + " not found");
    }
    // INTENT: config-owned sessions are reconciled from YAML on reload; a
    // client edit would be silently reverted and would affect every app on it.
    if (info.owner != "client") {
        return reply_err(c, h, CtlStatus::UNSUPPORTED, "session " + spec.name + " is config-owned");
    }
    if (!sessions_.set_filter(spec, err)) return reply_err(c, h, CtlStatus::BAD_REQUEST, err);
    if (!sessions_.attach_info(spec.name, info)) {
        return reply_err(c, h, CtlStatus::NOT_FOUND, "session " + spec.name + " vanished");
    }
    CtlSetFilterResp r {info.filter_generation};
    return reply(c, h, CtlStatus::OK, &r, sizeof(r));
}

bool ControlServer::op_destroy(Client& c, const CtlHeader& h, const uint8_t* body) {
    std::string name;
    if (!read_name_req(h, body, name)) return reply_err(c, h, CtlStatus::BAD_REQUEST, "DESTROY_SESSION: bad name");
    SessionAttachInfo info;
    if (!sessions_.attach_info(name, info)) {
        return reply_err(c, h, CtlStatus::NOT_FOUND, "session " + name + " not found");
    }
    if (info.owner != "client") {
        return reply_err(c, h, CtlStatus::UNSUPPORTED, "session " + name + " is config-owned");
    }
    std::string err;
    const bool ok = sessions_.destroy(name, 1, err);
    // The entry is gone either way (SessionManager erases it even if the ack
    // timed out); nobody holds it any more.
    release_everywhere(name);
    if (!ok) return reply_err(c, h, CtlStatus::INTERNAL, err);
    LogRegistry::instance().info("CONTROL", "SESSION_DESTROYED",
                                 name + " by uid=" + std::to_string(c.uid) + " pid=" + std::to_string(c.pid));
    return reply(c, h, CtlStatus::OK, nullptr, 0);
}

bool ControlServer::op_snapshot(Client& c, const CtlHeader& h, const uint8_t* body) {
    std::string name;
    if (!read_name_req(h, body, name)) return reply_err(c, h, CtlStatus::BAD_REQUEST, "SNAPSHOT: bad name");

    // Collect first: the leading reply carries the record count.
    constexpr size_t kMaxPayload = CTL_MAX_DATAGRAM - sizeof(CtlHeader) - sizeof(TickHeader);
    std::vector<std::vector<uint8_t>> dgrams;
    uint32_t skipped = 0;
    uint64_t head = 0;
    const bool found = sessions_.snapshot(name, head, [&](const TickHeader& th, const uint8_t* p, uint32_t len) {
        if (len > kMaxPayload) {
            ++skipped;
            return;
        }
        std::vector<uint8_t> d(sizeof(CtlHeader) + sizeof(TickHeader) + len);
        CtlHeader rh {};
        rh.len = static_cast<uint32_t>(sizeof(TickHeader) + len);
        rh.op = h.op;
        rh.version = CONTROL_PROTO_VERSION;
        rh.req_id = h.req_id;
        rh.status = 0;
        std::memcpy(d.data(), &rh, sizeof(rh));
        std::memcpy(d.data() + sizeof(rh), &th, sizeof(th));
        if (len != 0) std::memcpy(d.data() + sizeof(rh) + sizeof(th), p, len);
        dgrams.push_back(std::move(d));
    });
    if (!found) return reply_err(c, h, CtlStatus::NOT_FOUND, "session " + name + " not found");

    CtlSnapshotResp r {};
    r.ring_head_at_snapshot = head;
    r.record_count = static_cast<uint32_t>(dgrams.size());
    r.skipped = skipped;
    if (!reply(c, h, CtlStatus::OK, &r, sizeof(r))) return false;
    for (const auto& d : dgrams) {
        if (!send_dgram(c, d.data(), d.size())) return false;
    }
    CtlSnapshotEnd end {r.record_count, 1};
    return reply(c, h, CtlStatus::OK, &end, sizeof(end));
}

bool ControlServer::op_symbols(Client& c, const CtlHeader& h, const uint8_t* body) {
    if (h.len != sizeof(CtlSymbolsReq)) return reply_err(c, h, CtlStatus::BAD_REQUEST, "SYMBOLS: bad body");
    CtlSymbolsReq q {};
    std::memcpy(&q, body, sizeof(q));
    const uint32_t max = (q.max == 0 || q.max > CTL_SYMBOLS_PAGE_MAX) ? CTL_SYMBOLS_PAGE_MAX : q.max;
    const uint32_t last = symbols_.size(); // ids are dense from 1
    std::vector<CtlSymbolEntry> entries;
    uint32_t id = q.from_id == 0 ? 1 : q.from_id;
    std::string name;
    for (; id <= last && entries.size() < max; ++id) {
        if (!symbols_.resolve(id, name)) continue; // claimed but not yet published
        CtlSymbolEntry e {};
        e.id = id;
        e.len = static_cast<uint8_t>(std::min<size_t>(name.size(), CTL_SYMBOL_LEN));
        set_name(e.name, name);
        entries.push_back(e);
    }
    CtlSymbolsResp r {static_cast<uint32_t>(entries.size()), id};
    std::vector<uint8_t> out(sizeof(r) + entries.size() * sizeof(CtlSymbolEntry));
    std::memcpy(out.data(), &r, sizeof(r));
    if (!entries.empty()) std::memcpy(out.data() + sizeof(r), entries.data(), entries.size() * sizeof(CtlSymbolEntry));
    return reply(c, h, CtlStatus::OK, out.data(), out.size());
}

bool ControlServer::op_identify(Client& c, const CtlHeader& h, const uint8_t* body) {
    if (h.len != sizeof(CtlIdentifyReq)) return reply_err(c, h, CtlStatus::BAD_REQUEST, "IDENTIFY: bad body");
    CtlIdentifyReq q {};
    std::memcpy(&q, body, sizeof(q));
    const std::string_view name = get_name(q.app_name);
    bool ok = !name.empty() && name.size() < sizeof(q.app_name);
    for (char ch : name) ok = ok && ch >= 0x20 && ch < 0x7f && ch != '"' && ch != '\\';
    if (!ok) return reply_err(c, h, CtlStatus::BAD_REQUEST, "IDENTIFY: app name must be 1-31 printable chars, no quote or backslash");
    c.app_name = std::string(name);
    clients_dirty_ = true;
    LogRegistry::instance().info("CONTROL", "IDENTIFY",
                                 c.app_name + " uid=" + std::to_string(c.uid) + " pid=" + std::to_string(c.pid));
    return reply(c, h, CtlStatus::OK, nullptr, 0);
}

bool ControlServer::op_ping(Client& c, const CtlHeader& h, const uint8_t* body) {
    if (h.len != sizeof(CtlPingReq)) return reply_err(c, h, CtlStatus::BAD_REQUEST, "PING: bad body");
    CtlPingReq q {};
    std::memcpy(&q, body, sizeof(q));
    CtlPingResp r {q.client_ns, now_ns()};
    return reply(c, h, CtlStatus::OK, &r, sizeof(r));
}

} // namespace subplane
