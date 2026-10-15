#include "subplane/transport/udp_multicast.h"

#include "subplane/core/clock.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#ifndef SO_BUSY_POLL
#define SO_BUSY_POLL 46
#endif

namespace subplane {

bool UdpMulticastConfig::parse(const YAML::Node& node, UdpMulticastConfig& out, std::string& err) {
    try {
        out.group = node["group"].as<std::string>("");
        int port = node["port"].as<int>(0);
        if (port <= 0 || port > 65535) {
            err = "transport.port must be 1..65535";
            return false;
        }
        out.port = static_cast<uint16_t>(port);
        out.interface = node["interface"].as<std::string>("");
        out.rcvbuf = node["rcvbuf"].as<int>(out.rcvbuf);
        out.busy_poll_us = node["busy_poll_us"].as<int>(0);
        out.mtu = node["mtu"].as<uint32_t>(1500u);
        out.batch = node["batch"].as<int>(32);
        out.kernel_timestamps = node["kernel_timestamps"].as<bool>(false);
        out.multicast_loop = node["multicast_loop"].as<bool>(true);
        out.reuse_port = node["reuse_port"].as<bool>(true);
    } catch (const YAML::Exception& e) {
        err = std::string("transport config: ") + e.what();
        return false;
    }
    if (out.batch < 1 || out.batch > PacketBatch::kMax) {
        err = "transport.batch must be 1.." + std::to_string(PacketBatch::kMax);
        return false;
    }
    if (out.mtu < 64 || out.mtu > 65535) {
        err = "transport.mtu must be 64..65535";
        return false;
    }
    if (!out.group.empty()) {
        in_addr a{};
        if (inet_pton(AF_INET, out.group.c_str(), &a) != 1 || (ntohl(a.s_addr) >> 28) != 0xE) {
            err = "transport.group is not an IPv4 multicast address: " + out.group;
            return false;
        }
    }
    return true;
}

std::unique_ptr<Transport> UdpMulticastTransport::create(const YAML::Node& cfg, std::string& err) {
    UdpMulticastConfig c;
    if (!UdpMulticastConfig::parse(cfg, c, err)) {
        return nullptr;
    }
    return std::make_unique<UdpMulticastTransport>(std::move(c));
}

UdpMulticastTransport::UdpMulticastTransport(UdpMulticastConfig cfg) : cfg_(std::move(cfg)) {}

UdpMulticastTransport::~UdpMulticastTransport() {
    close();
}

bool UdpMulticastTransport::open(std::string& err) {
    if (fd_ >= 0) {
        err = "already open";
        return false;
    }
    fd_ = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd_ < 0) {
        err = std::string("socket: ") + strerror(errno);
        return false;
    }

    int one = 1;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    if (cfg_.reuse_port) {
        ::setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
    }
    if (cfg_.rcvbuf > 0) {
        ::setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &cfg_.rcvbuf, sizeof(cfg_.rcvbuf));
    }
    {
        int actual = 0;
        socklen_t len = sizeof(actual);
        if (::getsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &actual, &len) == 0) {
            effective_rcvbuf_ = actual; // kernel reports doubled value; informational
        }
    }
    if (cfg_.busy_poll_us > 0) {
        // INTENT: busy-poll needs driver support and may need CAP_NET_ADMIN on
        // older kernels. Failure is not fatal: degrade and report via busy_poll_active().
        busy_poll_active_ = ::setsockopt(fd_, SOL_SOCKET, SO_BUSY_POLL, &cfg_.busy_poll_us,
                                         sizeof(cfg_.busy_poll_us)) == 0;
    }
    if (cfg_.kernel_timestamps) {
        ::setsockopt(fd_, SOL_SOCKET, SO_TIMESTAMPNS, &one, sizeof(one));
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(cfg_.port);
    // INTENT: bind to the group address (not INADDR_ANY) when multicast so a
    // socket on the same port for a different group does not receive our packets.
    if (!cfg_.group.empty()) {
        inet_pton(AF_INET, cfg_.group.c_str(), &addr.sin_addr);
    } else if (!cfg_.interface.empty()) {
        if (inet_pton(AF_INET, cfg_.interface.c_str(), &addr.sin_addr) != 1) {
            err = "bad interface address: " + cfg_.interface;
            close();
            return false;
        }
    } else {
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
    }
    if (::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        err = std::string("bind: ") + strerror(errno);
        close();
        return false;
    }

    if (!cfg_.group.empty()) {
        ip_mreq mreq{};
        inet_pton(AF_INET, cfg_.group.c_str(), &mreq.imr_multiaddr);
        if (!cfg_.interface.empty()) {
            if (inet_pton(AF_INET, cfg_.interface.c_str(), &mreq.imr_interface) != 1) {
                err = "bad interface address: " + cfg_.interface;
                close();
                return false;
            }
        } else {
            mreq.imr_interface.s_addr = htonl(INADDR_ANY);
        }
        if (::setsockopt(fd_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0) {
            err = std::string("IP_ADD_MEMBERSHIP: ") + strerror(errno);
            close();
            return false;
        }
        unsigned char loop = cfg_.multicast_loop ? 1 : 0;
        ::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop));
    }

    const auto batch = static_cast<size_t>(cfg_.batch);
    const size_t cmsg_space = CMSG_SPACE(sizeof(timespec));
    arena_.assign(batch * cfg_.mtu, 0);
    iov_.assign(batch, iovec{});
    msgs_.assign(batch, mmsghdr{});
    cmsg_.assign(batch * cmsg_space, 0);
    for (size_t i = 0; i < batch; ++i) {
        iov_[i].iov_base = arena_.data() + i * cfg_.mtu;
        iov_[i].iov_len = cfg_.mtu;
        msgs_[i].msg_hdr.msg_iov = &iov_[i];
        msgs_[i].msg_hdr.msg_iovlen = 1;
        msgs_[i].msg_hdr.msg_control = cfg_.kernel_timestamps ? cmsg_.data() + i * cmsg_space : nullptr;
        msgs_[i].msg_hdr.msg_controllen = cfg_.kernel_timestamps ? cmsg_space : 0;
    }
    return true;
}

int UdpMulticastTransport::poll(PacketBatch& out) noexcept {
    out.count = 0;
    if (fd_ < 0) {
        return -1;
    }
    // Reset per-call fields the kernel mutates.
    const auto batch = static_cast<unsigned>(cfg_.batch);
    for (unsigned i = 0; i < batch; ++i) {
        msgs_[i].msg_hdr.msg_flags = 0;
        if (cfg_.kernel_timestamps) {
            msgs_[i].msg_hdr.msg_controllen = CMSG_SPACE(sizeof(timespec));
        }
    }
    int n = ::recvmmsg(fd_, msgs_.data(), batch, MSG_DONTWAIT, nullptr);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            return 0;
        }
        report_down(TransportReason::LOCAL_FAILURE);
        return -1;
    }
    // INTENT: UDP has no connection; the first datagram, or the first after a
    // socket error, is what "up" means. A silent sender is the heartbeat
    // timeout's job (PARTITION_DEGRADED), not a transport state.
    if (n > 0) report_up();
    // INTENT: one clock read per batch, not per packet, unless the kernel
    // stamped each datagram. Keeps the per-packet cost to the memcpy the
    // kernel already did.
    const uint64_t batch_ts = cfg_.kernel_timestamps ? 0 : now_ns();
    for (int i = 0; i < n; ++i) {
        auto& m = msgs_[static_cast<size_t>(i)];
        PacketView& p = out.pkts[i];
        p.data = static_cast<const uint8_t*>(m.msg_hdr.msg_iov->iov_base);
        p.len = m.msg_len;
        p.recv_ts_ns = batch_ts;
        if (cfg_.kernel_timestamps) {
            for (cmsghdr* c = CMSG_FIRSTHDR(&m.msg_hdr); c != nullptr; c = CMSG_NXTHDR(&m.msg_hdr, c)) {
                if (c->cmsg_level == SOL_SOCKET && c->cmsg_type == SO_TIMESTAMPNS) {
                    timespec ts{};
                    std::memcpy(&ts, CMSG_DATA(c), sizeof(ts));
                    p.recv_ts_ns = static_cast<uint64_t>(ts.tv_sec) * 1000000000ull +
                                   static_cast<uint64_t>(ts.tv_nsec);
                    break;
                }
            }
            if (p.recv_ts_ns == 0) {
                p.recv_ts_ns = now_ns();
            }
        }
    }
    out.count = n;
    return n;
}

void UdpMulticastTransport::close() noexcept {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

std::string UdpMulticastTransport::describe() const {
    std::string s = cfg_.group.empty() ? "udp://" : "udp-mcast://" + cfg_.group + "@";
    if (cfg_.group.empty()) {
        s += cfg_.interface.empty() ? "0.0.0.0" : cfg_.interface;
    } else if (!cfg_.interface.empty()) {
        s += cfg_.interface;
    } else {
        s += "any";
    }
    return s + ":" + std::to_string(cfg_.port);
}

} // namespace subplane
