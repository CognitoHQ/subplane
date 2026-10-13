#pragma once

// UDP multicast (or plain UDP unicast when group is empty) transport.
// recvmmsg batching, SO_RCVBUF, optional SO_BUSY_POLL and SO_TIMESTAMPNS.

#include "subplane/transport/transport.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <sys/socket.h>

namespace subplane {

struct UdpMulticastConfig {
    std::string group;        // multicast group, e.g. 239.1.1.1. Empty = unicast UDP.
    uint16_t    port = 0;
    std::string interface;    // local interface address for membership/bind. Empty = INADDR_ANY.
    int         rcvbuf = 16 * 1024 * 1024;
    int         busy_poll_us = 0;   // SO_BUSY_POLL; 0 = off
    uint32_t    mtu = 1500;         // max datagram bytes to read
    int         batch = 32;         // recvmmsg batch, <= PacketBatch::kMax
    bool        kernel_timestamps = false; // SO_TIMESTAMPNS
    bool        multicast_loop = true;     // IP_MULTICAST_LOOP (needed for same-host tests)
    bool        reuse_port = true;

    // Parses the `transport:` YAML block. Returns false with err on bad values.
    static bool parse(const YAML::Node& node, UdpMulticastConfig& out, std::string& err);
};

class UdpMulticastTransport final : public Transport {
public:
    explicit UdpMulticastTransport(UdpMulticastConfig cfg);
    ~UdpMulticastTransport() override;

    bool open(std::string& err) override;
    int  poll(PacketBatch& out) noexcept override;
    void close() noexcept override;
    std::string describe() const override;
    int fd() const noexcept override { return fd_; }

    // Effective receive buffer after the kernel clamped it (0 before open).
    int effective_rcvbuf() const noexcept { return effective_rcvbuf_; }
    bool busy_poll_active() const noexcept { return busy_poll_active_; }

    // Registry factory.
    static std::unique_ptr<Transport> create(const YAML::Node& cfg, std::string& err);

private:
    UdpMulticastConfig cfg_;
    int fd_ = -1;
    int effective_rcvbuf_ = 0;
    bool busy_poll_active_ = false;

    // INTENT: all receive buffers are allocated once in open() and reused.
    // poll() must not allocate: it is called on the pinned pipeline thread.
    std::vector<uint8_t>     arena_;   // batch * mtu bytes
    std::vector<iovec>       iov_;
    std::vector<mmsghdr>     msgs_;
    std::vector<uint8_t>     cmsg_;    // batch * cmsg space (for kernel timestamps)
};

} // namespace subplane
