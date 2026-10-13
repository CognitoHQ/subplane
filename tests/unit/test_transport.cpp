#include <doctest.h>

#include "flashtick/registry/component_registry.h"
#include "flashtick/transport/udp_multicast.h"

#include <arpa/inet.h>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

using namespace flashtick;

namespace {

// Binds a unicast UDP receiver on an ephemeral loopback port and returns the port.
uint16_t pick_free_port() {
    int s = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = 0;
    REQUIRE(::bind(s, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0);
    socklen_t len = sizeof(a);
    REQUIRE(::getsockname(s, reinterpret_cast<sockaddr*>(&a), &len) == 0);
    uint16_t port = ntohs(a.sin_port);
    ::close(s);
    return port;
}

void send_udp(const char* host, uint16_t port, const void* data, size_t len) {
    int s = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(port);
    inet_pton(AF_INET, host, &a.sin_addr);
    REQUIRE(::sendto(s, data, len, 0, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == static_cast<ssize_t>(len));
    ::close(s);
}

} // namespace

TEST_CASE("UdpMulticastConfig parses and validates YAML") {
    std::string err;
    UdpMulticastConfig c;
    CHECK(UdpMulticastConfig::parse(YAML::Load("{group: 239.1.1.1, port: 30001, batch: 8}"), c, err));
    CHECK(c.group == "239.1.1.1");
    CHECK(c.port == 30001);
    CHECK(c.batch == 8);

    CHECK_FALSE(UdpMulticastConfig::parse(YAML::Load("{group: 10.0.0.1, port: 1}"), c, err));
    CHECK(err.find("multicast") != std::string::npos);
    CHECK_FALSE(UdpMulticastConfig::parse(YAML::Load("{port: 0}"), c, err));
    CHECK_FALSE(UdpMulticastConfig::parse(YAML::Load("{port: 5, batch: 1000}"), c, err));
}

TEST_CASE("registry constructs udp_multicast and rejects unknown types") {
    std::string err;
    auto t = make_component(transport_components(), "udp_multicast", YAML::Load("{group: '', port: 4000}"), err);
    REQUIRE(t);
    CHECK(t->describe() == "udp://0.0.0.0:4000");

    auto bad = make_component(transport_components(), "carrier_pigeon", YAML::Load("{}"), err);
    CHECK_FALSE(bad);
    CHECK(err.find("udp_multicast") != std::string::npos);
}

#if FLASHTICK_WITH_AERON
TEST_CASE("aeron transport config: required keys and bounds, no driver needed") {
    std::string err;
    CHECK_FALSE(make_component(transport_components(), "aeron", YAML::Load("{stream_id: 1}"), err));
    CHECK(err.find("channel") != std::string::npos);
    CHECK_FALSE(make_component(transport_components(), "aeron", YAML::Load("{channel: 'aeron:ipc'}"), err));
    CHECK(err.find("stream_id") != std::string::npos);
    CHECK_FALSE(make_component(transport_components(), "aeron", YAML::Load("{channel: 'aeron:ipc', stream_id: 1, batch: 65}"), err));
    CHECK(err.find("batch") != std::string::npos);
    CHECK_FALSE(make_component(transport_components(), "aeron", YAML::Load("{channel: 'aeron:ipc', stream_id: 1, driver: {threading_mode: turbo}}"), err));
    CHECK(err.find("threading_mode") != std::string::npos);
    auto t = make_component(transport_components(), "aeron", YAML::Load("{channel: 'aeron:ipc', stream_id: 7, driver: {embedded: true}}"), err);
    REQUIRE(t);
    CHECK(t->describe() == "aeron aeron:ipc stream 7 (embedded driver)");
    CHECK(t->fd() == -1);
}
#endif

TEST_CASE("unicast UDP receive with recvmmsg batching and timestamps") {
    uint16_t port = pick_free_port();
    UdpMulticastConfig c;
    c.group = "";
    c.interface = "127.0.0.1";
    c.port = port;
    c.batch = 4;
    c.mtu = 256;
    UdpMulticastTransport t(c);
    std::string err;
    REQUIRE_MESSAGE(t.open(err), err);

    PacketBatch b;
    CHECK(t.poll(b) == 0); // nothing yet, non-blocking

    for (int i = 0; i < 6; ++i) {
        char msg[16];
        snprintf(msg, sizeof(msg), "pkt-%d", i);
        send_udp("127.0.0.1", port, msg, strlen(msg));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    int total = 0;
    uint64_t last_ts = 0;
    for (int tries = 0; tries < 10 && total < 6; ++tries) {
        int n = t.poll(b);
        REQUIRE(n >= 0);
        CHECK(n <= 4);
        for (int i = 0; i < n; ++i) {
            char expect[16];
            snprintf(expect, sizeof(expect), "pkt-%d", total + i);
            CHECK(b.pkts[i].len == strlen(expect));
            CHECK(std::memcmp(b.pkts[i].data, expect, b.pkts[i].len) == 0);
            CHECK(b.pkts[i].recv_ts_ns > 0);
            last_ts = b.pkts[i].recv_ts_ns;
        }
        total += n;
    }
    CHECK(total == 6);
    CHECK(last_ts > 0);
    t.close();
    CHECK(t.poll(b) == -1);
}

TEST_CASE("multicast loopback receive (skipped if the host has no multicast route)") {
    uint16_t port = pick_free_port();
    UdpMulticastConfig c;
    c.group = "239.255.77.77";
    c.interface = "127.0.0.1";
    c.port = port;
    UdpMulticastTransport t(c);
    std::string err;
    if (!t.open(err)) {
        MESSAGE("multicast open failed, skipping: " << err);
        return;
    }
    // Sender must choose the loopback interface for the multicast egress.
    int s = ::socket(AF_INET, SOCK_DGRAM, 0);
    in_addr ifa{};
    inet_pton(AF_INET, "127.0.0.1", &ifa);
    ::setsockopt(s, IPPROTO_IP, IP_MULTICAST_IF, &ifa, sizeof(ifa));
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(port);
    inet_pton(AF_INET, "239.255.77.77", &a.sin_addr);
    const char payload[] = "mcast";
    if (::sendto(s, payload, sizeof(payload) - 1, 0, reinterpret_cast<sockaddr*>(&a), sizeof(a)) < 0) {
        MESSAGE("multicast send failed, skipping: " << strerror(errno));
        ::close(s);
        return;
    }
    ::close(s);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    PacketBatch b;
    int n = 0;
    for (int tries = 0; tries < 10 && n == 0; ++tries) {
        n = t.poll(b);
        if (n == 0) std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    if (n <= 0) {
        MESSAGE("no multicast packet received on loopback; environment-dependent, skipping");
        return;
    }
    CHECK(b.pkts[0].len == 5);
    CHECK(std::memcmp(b.pkts[0].data, "mcast", 5) == 0);
}
