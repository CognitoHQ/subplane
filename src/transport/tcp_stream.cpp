#include "subplane/transport/tcp_stream.h"

#include "subplane/core/counters.h"
#include "subplane/observability/endpoint_key.h"

#include "subplane/core/clock.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace subplane {

bool TcpStreamConfig::parse(const YAML::Node& node, TcpStreamConfig& out, std::string& err) {
    try {
        out = TcpStreamConfig{};
        out.host = node["host"].as<std::string>("127.0.0.1");
        const int port = node["port"].as<int>(0);
        if (port <= 0 || port > 65535) { err = "transport.port must be 1..65535"; return false; }
        out.port = static_cast<uint16_t>(port);
        if (const auto rl = node["record_length"]) {
            if (!rl.IsMap()) { err = "transport.record_length must be a map {offset, size, scale}"; return false; }
            out.len_offset = rl["offset"].as<uint32_t>(0u);
            const int sz = rl["size"].as<int>(1);
            if (sz != 1 && sz != 2 && sz != 4) { err = "transport.record_length.size must be 1, 2 or 4"; return false; }
            out.len_size = static_cast<uint8_t>(sz);
            out.len_scale = rl["scale"].as<uint32_t>(1u);
            if (out.len_scale == 0) { err = "transport.record_length.scale must be >= 1"; return false; }
            out.len_add = rl["add"].as<int32_t>(0);
            if (out.len_add < -255 || out.len_add > 255) { err = "transport.record_length.add must be within -255..255"; return false; }
        }
        const std::string pre = node["prelude"].as<std::string>("none");
        if (pre == "none") out.prelude = StreamPrelude::NONE;
        else if (pre == "dbn") out.prelude = StreamPrelude::DBN;
        else { err = "transport.prelude must be none or dbn, got '" + pre + "'"; return false; }
        out.rcvbuf = node["rcvbuf"].as<uint32_t>(out.rcvbuf);
        out.batch = node["batch"].as<int>(PacketBatch::kMax);
        out.max_record = node["max_record"].as<uint32_t>(out.max_record);
        out.reconnect_ms = node["reconnect_ms"].as<uint32_t>(out.reconnect_ms);
        out.connect_timeout_ms = node["connect_timeout_ms"].as<uint32_t>(out.connect_timeout_ms);
    } catch (const YAML::Exception& e) {
        err = std::string("transport config: ") + e.what();
        return false;
    }
    if (out.batch < 1 || out.batch > PacketBatch::kMax) { err = "transport.batch must be 1.." + std::to_string(PacketBatch::kMax); return false; }
    if (out.max_record < out.len_offset + out.len_size || out.max_record > (1u << 24)) { err = "transport.max_record out of range"; return false; }
    if (out.rcvbuf < 2u * out.max_record) { err = "transport.rcvbuf must be at least 2 * max_record"; return false; }
    return true;
}

std::unique_ptr<Transport> TcpStreamTransport::create(const YAML::Node& cfg, std::string& err) {
    TcpStreamConfig c;
    if (!TcpStreamConfig::parse(cfg, c, err)) return nullptr;
    return std::make_unique<TcpStreamTransport>(std::move(c));
}

TcpStreamTransport::TcpStreamTransport(TcpStreamConfig cfg) : cfg_(std::move(cfg)) {}
TcpStreamTransport::~TcpStreamTransport() { close(); }

bool TcpStreamTransport::open(std::string& err) {
    if (!arena_.empty()) { err = "already open"; return false; }
    addr_ = sockaddr_in{};
    addr_.sin_family = AF_INET;
    addr_.sin_port = htons(cfg_.port);
    if (inet_pton(AF_INET, cfg_.host.c_str(), &addr_.sin_addr) != 1) {
        addrinfo hints{};
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        addrinfo* res = nullptr;
        const int rc = getaddrinfo(cfg_.host.c_str(), nullptr, &hints, &res);
        if (rc != 0 || !res) { err = "resolve " + cfg_.host + ": " + gai_strerror(rc); return false; }
        addr_.sin_addr = reinterpret_cast<sockaddr_in*>(res->ai_addr)->sin_addr;
        freeaddrinfo(res);
    }
    arena_.assign(cfg_.rcvbuf, 0);
    rd_ = wr_ = 0;
    state_ = State::DISCONNECTED;
    next_connect_ns_ = 0;
    if (!start_connect() && cfg_.reconnect_ms == 0) {
        err = "connect " + cfg_.host + ":" + std::to_string(cfg_.port) + ": " + last_error_;
        close();
        return false;
    }
    return true;
}

bool TcpStreamTransport::start_connect() noexcept {
    fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd_ < 0) { last_error_ = std::string("socket: ") + strerror(errno); report_down(TransportReason::LOCAL_FAILURE); state_ = State::DISCONNECTED; next_connect_ns_ = now_ns() + cfg_.reconnect_ms * 1'000'000ull; return false; }
    int one = 1;
    ::setsockopt(fd_, IPPROTO_TCP, 1 /* TCP_NODELAY */, &one, sizeof(one));
    const int rb = static_cast<int>(cfg_.rcvbuf);
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &rb, sizeof(rb));
    rd_ = wr_ = 0;
    prelude_pending_ = cfg_.prelude != StreamPrelude::NONE;
    prelude_skip_ = 0;
    const int rc = ::connect(fd_, reinterpret_cast<sockaddr*>(&addr_), sizeof(addr_));
    if (rc == 0) {
        state_ = State::CONNECTED;
        inc(connects_);
        report_up();
        return true;
    }
    if (errno == EINPROGRESS) {
        state_ = State::CONNECTING;
        connect_deadline_ns_ = now_ns() + cfg_.connect_timeout_ms * 1'000'000ull;
        return true;
    }
    const int cerr = errno;
    last_error_ = std::string("connect: ") + strerror(cerr);
    report_down(cerr == ECONNREFUSED ? TransportReason::SOURCE_LOST : TransportReason::LOCAL_FAILURE);
    ::close(fd_);
    fd_ = -1;
    state_ = State::DISCONNECTED;
    next_connect_ns_ = now_ns() + cfg_.reconnect_ms * 1'000'000ull;
    return false;
}

void TcpStreamTransport::drop(const char* why, TransportReason reason) noexcept {
    last_error_ = why;
    report_down(reason);
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
    if (state_ == State::CONNECTED) inc(disconnects_);
    rd_ = wr_ = 0;
    if (cfg_.reconnect_ms == 0) { state_ = State::FATAL; return; }
    state_ = State::DISCONNECTED;
    next_connect_ns_ = now_ns() + cfg_.reconnect_ms * 1'000'000ull;
}

uint32_t TcpStreamTransport::record_len_at(const uint8_t* p) const noexcept {
    const uint8_t* q = p + cfg_.len_offset;
    uint64_t v = 0;
    switch (cfg_.len_size) {
        case 1: v = q[0]; break;
        case 2: { uint16_t x; std::memcpy(&x, q, 2); v = x; break; }
        default: { uint32_t x; std::memcpy(&x, q, 4); v = x; break; }
    }
    v *= cfg_.len_scale;
    if (cfg_.len_add < 0 && v < static_cast<uint64_t>(-cfg_.len_add)) return 0; // reported as a framing error by the caller
    v = static_cast<uint64_t>(static_cast<int64_t>(v) + cfg_.len_add);
    return v > 0xFFFFFFFFull ? 0xFFFFFFFFu : static_cast<uint32_t>(v);
}

bool TcpStreamTransport::consume_prelude() noexcept {
    // DBN: "DBN" u8 version u32 metadata_len, then metadata_len bytes.
    if (prelude_skip_ == 0) {
        if (wr_ - rd_ < 8) return false;
        const uint8_t* p = arena_.data() + rd_;
        if (std::memcmp(p, "DBN", 3) != 0) {
            uint32_t magic; std::memcpy(&magic, p, 4);
            if (magic == 0xFD2FB528u) {
                last_error_ = "stream is zstd-compressed DBN; decompress it (dbn CLI) or serve raw DBN";
            } else {
                last_error_ = "stream does not start with a DBN prelude";
            }
            if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
            inc(framing_errors_);
            report_down(TransportReason::LOCAL_FAILURE);
            state_ = State::FATAL; // a wrong stream will not fix itself by reconnecting
            return false;
        }
        uint32_t mlen; std::memcpy(&mlen, p + 4, 4);
        rd_ += 8;
        prelude_skip_ = mlen;
        if (prelude_skip_ == 0) { prelude_pending_ = false; return true; }
    }
    const size_t have = wr_ - rd_;
    const size_t take = have < prelude_skip_ ? have : prelude_skip_;
    rd_ += take;
    prelude_skip_ -= take;
    if (prelude_skip_ == 0) { prelude_pending_ = false; return true; }
    return false;
}

int TcpStreamTransport::poll(PacketBatch& out) noexcept {
    out.count = 0;
    if (arena_.empty()) return -1;
    switch (state_) {
        case State::FATAL:
            return -1;
        case State::DISCONNECTED:
            if (now_ns() < next_connect_ns_) return 0;
            start_connect();
            if (state_ != State::CONNECTED) return 0;
            break;
        case State::CONNECTING: {
            pollfd pfd{fd_, POLLOUT, 0};
            const int pr = ::poll(&pfd, 1, 0);
            if (pr == 0) {
                if (now_ns() > connect_deadline_ns_) drop("connect timeout", TransportReason::LOCAL_FAILURE);
                return 0;
            }
            int soerr = 0; socklen_t sl = sizeof(soerr);
            if (pr < 0 || ::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &soerr, &sl) < 0 || soerr != 0) {
                drop(soerr ? strerror(soerr) : "connect failed", soerr == ECONNREFUSED ? TransportReason::SOURCE_LOST : TransportReason::LOCAL_FAILURE);
                return 0;
            }
            state_ = State::CONNECTED;
            inc(connects_);
            report_up();
            break;
        }
        case State::CONNECTED:
            break;
    }

    // Compact when the free tail cannot hold a maximum record.
    if (rd_ > 0 && arena_.size() - wr_ < cfg_.max_record) {
        std::memmove(arena_.data(), arena_.data() + rd_, wr_ - rd_);
        wr_ -= rd_;
        rd_ = 0;
    }
    const size_t room = arena_.size() - wr_;
    if (room > 0) {
        const ssize_t n = ::read(fd_, arena_.data() + wr_, room);
        if (n == 0) { drop("peer closed", TransportReason::SOURCE_LOST); return state_ == State::FATAL ? -1 : 0; }
        if (n < 0) {
            const int rerr = errno;
            if (rerr != EAGAIN && rerr != EWOULDBLOCK && rerr != EINTR) {
                drop(strerror(rerr), rerr == ECONNRESET || rerr == EPIPE ? TransportReason::SOURCE_LOST : TransportReason::LOCAL_FAILURE);
                return state_ == State::FATAL ? -1 : 0;
            }
        } else {
            wr_ += static_cast<size_t>(n);
        }
    }

    if (prelude_pending_ && !consume_prelude()) return state_ == State::FATAL ? -1 : 0;

    const uint64_t ts = now_ns();
    const size_t hdr_need = cfg_.len_offset + cfg_.len_size;
    while (out.count < cfg_.batch && wr_ - rd_ >= hdr_need) {
        const uint8_t* p = arena_.data() + rd_;
        const uint32_t len = record_len_at(p);
        if (len < hdr_need || len > cfg_.max_record) {
            inc(framing_errors_);
            drop("record length out of range (stream misaligned?)", TransportReason::LOCAL_FAILURE);
            return state_ == State::FATAL ? -1 : out.count;
        }
        if (wr_ - rd_ < len) break; // partial record: wait for more bytes
        out.pkts[out.count++] = PacketView{p, len, ts};
        rd_ += len;
    }
    return out.count;
}

void TcpStreamTransport::close() noexcept {
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
    arena_.clear();
    arena_.shrink_to_fit();
    rd_ = wr_ = 0;
    state_ = State::DISCONNECTED;
}

std::string TcpStreamTransport::endpoint_key(std::string_view host) const {
    return observability::tcp_key(cfg_.host, cfg_.port, host);
}

std::string TcpStreamTransport::describe() const {
    const char* st = state_ == State::CONNECTED ? "connected" : state_ == State::CONNECTING ? "connecting"
                   : state_ == State::FATAL ? "fatal" : "disconnected";
    return "tcp_stream " + cfg_.host + ":" + std::to_string(cfg_.port) + " (" + st + ")";
}

} // namespace subplane
