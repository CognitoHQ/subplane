#include <doctest.h>

#include "subplane/registry/component_registry.h"
#include "subplane/transport/tcp_stream.h"

#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace subplane;

namespace {

// Minimal blocking TCP listener on an ephemeral loopback port.
struct Listener {
    int lfd = -1;
    uint16_t port = 0;
    Listener() {
        lfd = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        REQUIRE(::bind(lfd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0);
        socklen_t len = sizeof(a);
        REQUIRE(::getsockname(lfd, reinterpret_cast<sockaddr*>(&a), &len) == 0);
        port = ntohs(a.sin_port);
        REQUIRE(::listen(lfd, 2) == 0);
    }
    ~Listener() { if (lfd >= 0) ::close(lfd); }
    int accept() { return ::accept(lfd, nullptr, nullptr); }
};

void send_chunk(int fd, const std::vector<uint8_t>& v, size_t from, size_t to) {
    const uint8_t* p = v.data() + from;
    size_t n = to - from;
    while (n > 0) {
        const ssize_t w = ::send(fd, p, n, MSG_NOSIGNAL);
        REQUIRE(w > 0);
        p += w; n -= static_cast<size_t>(w);
    }
}

std::vector<uint8_t> dbn_record(uint8_t rtype, uint32_t total, uint8_t fill) {
    std::vector<uint8_t> r(total, fill);
    r[0] = static_cast<uint8_t>(total / 4);
    r[1] = rtype;
    return r;
}

std::vector<uint8_t> dbn_prelude(uint32_t metadata_len) {
    std::vector<uint8_t> p = {'D', 'B', 'N', 3};
    for (int i = 0; i < 4; ++i) p.push_back(static_cast<uint8_t>(metadata_len >> (8 * i)));
    p.insert(p.end(), metadata_len, 0xEE);
    return p;
}

TcpStreamConfig dbn_cfg(uint16_t port) {
    TcpStreamConfig c;
    c.host = "127.0.0.1";
    c.port = port;
    c.len_offset = 0; c.len_size = 1; c.len_scale = 4;
    c.prelude = StreamPrelude::DBN;
    c.rcvbuf = 1 << 16;
    c.max_record = 1024;
    c.reconnect_ms = 50;
    return c;
}

// Polls until `want` records arrive or the deadline passes; appends to out.
int collect(TcpStreamTransport& t, std::vector<std::vector<uint8_t>>& out, size_t want, int timeout_ms = 2000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    int last = 0;
    while (out.size() < want && std::chrono::steady_clock::now() < deadline) {
        PacketBatch b;
        last = t.poll(b);
        if (last < 0) return last;
        for (int i = 0; i < b.count; ++i) out.emplace_back(b.pkts[i].data, b.pkts[i].data + b.pkts[i].len);
        if (last == 0) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return last;
}

} // namespace

TEST_CASE("tcp_stream: config parsing and registry") {
    std::string err;
    TcpStreamConfig c;
    CHECK(TcpStreamConfig::parse(YAML::Load("{host: 127.0.0.1, port: 9550, record_length: {offset: 0, size: 1, scale: 4}, prelude: dbn}"), c, err));
    CHECK(c.len_scale == 4);
    CHECK(c.prelude == StreamPrelude::DBN);
    CHECK_FALSE(TcpStreamConfig::parse(YAML::Load("{port: 0}"), c, err));
    CHECK_FALSE(TcpStreamConfig::parse(YAML::Load("{port: 1, record_length: {size: 3}}"), c, err));
    CHECK_FALSE(TcpStreamConfig::parse(YAML::Load("{port: 1, prelude: sbe}"), c, err));
    CHECK_FALSE(TcpStreamConfig::parse(YAML::Load("{port: 1, rcvbuf: 1000, max_record: 65536}"), c, err));
    auto t = make_component(transport_components(), "tcp_stream", YAML::Load("{host: 127.0.0.1, port: 9550}"), err);
    REQUIRE(t);
    CHECK(t->describe().rfind("tcp_stream 127.0.0.1:9550", 0) == 0);
}

TEST_CASE("tcp_stream: reassembles whole records across arbitrary chunking, skipping the DBN prelude") {
    Listener ls;
    TcpStreamTransport t(dbn_cfg(ls.port));
    std::string err;
    REQUIRE_MESSAGE(t.open(err), err);

    // Stream: prelude(8+37) + rec A(56) + rec B(48) + rec C(80) + rec D(16).
    std::vector<uint8_t> stream = dbn_prelude(37);
    const auto A = dbn_record(0xA0, 56, 0xA1), B = dbn_record(0x00, 48, 0xB2), C = dbn_record(0x01, 80, 0xC3), D = dbn_record(0x17, 16, 0xD4);
    for (const auto* r : {&A, &B, &C, &D}) stream.insert(stream.end(), r->begin(), r->end());

    std::thread server([&] {
        int c = ls.accept();
        REQUIRE(c >= 0);
        // Awkward cuts: inside the prelude magic, inside metadata, a record header split
        // across writes, one write holding the tail of B plus all of C plus the head of D.
        const size_t cuts[] = {2, 20, 45 + 3, 45 + 56 + 30, 45 + 56 + 48 + 80 + 5, stream.size()};
        size_t from = 0;
        for (size_t to : cuts) {
            send_chunk(c, stream, from, to);
            from = to;
            std::this_thread::sleep_for(std::chrono::milliseconds(15));
        }
        ::close(c);
    });

    std::vector<std::vector<uint8_t>> got;
    REQUIRE(collect(t, got, 4) >= 0);
    server.join();
    REQUIRE(got.size() == 4);
    CHECK(got[0] == A);
    CHECK(got[1] == B);
    CHECK(got[2] == C);
    CHECK(got[3] == D);
    CHECK(t.connects() == 1);
    CHECK(t.framing_errors() == 0);
}

TEST_CASE("tcp_stream: prelude none, batch limit, then peer close and reconnect to a new listener") {
    Listener ls;
    TcpStreamConfig c = dbn_cfg(ls.port);
    c.prelude = StreamPrelude::NONE;
    c.batch = 3;
    TcpStreamTransport t(c);
    std::string err;
    REQUIRE_MESSAGE(t.open(err), err);

    std::vector<uint8_t> stream;
    for (int i = 0; i < 7; ++i) { auto r = dbn_record(0xA0, 24, static_cast<uint8_t>(i)); stream.insert(stream.end(), r.begin(), r.end()); }
    std::thread server([&] { int cfd = ls.accept(); REQUIRE(cfd >= 0); send_chunk(cfd, stream, 0, stream.size()); std::this_thread::sleep_for(std::chrono::milliseconds(50)); ::close(cfd); });

    std::vector<std::vector<uint8_t>> got;
    // First poll that returns data must respect batch = 3.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    PacketBatch b;
    int n = 0;
    while (n == 0 && std::chrono::steady_clock::now() < deadline) { n = t.poll(b); if (n == 0) std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
    REQUIRE(n == 3);
    for (int i = 0; i < n; ++i) got.emplace_back(b.pkts[i].data, b.pkts[i].data + b.pkts[i].len);
    REQUIRE(collect(t, got, 7) >= 0);
    server.join();
    REQUIRE(got.size() == 7);
    for (int i = 0; i < 7; ++i) CHECK(got[static_cast<size_t>(i)][20] == static_cast<uint8_t>(i));

    CHECK(t.state() == TransportState::UP);
    REQUIRE(t.state_change_count() == 1);
    CHECK(t.state_change(0).state == TransportState::UP);
    t.clear_state_changes();

    // Peer closed: poll reports 0 (not fatal) and the transport reconnects.
    std::vector<std::vector<uint8_t>> more;
    collect(t, more, 1, 100);
    CHECK(more.empty());
    CHECK(t.disconnects() == 1); // it may already have reconnected to the listener's backlog
    REQUIRE(t.state_change_count() >= 1);
    CHECK(t.state_change(0).state == TransportState::DOWN);
    CHECK(t.state_change(0).reason == TransportReason::SOURCE_LOST);

    std::thread server2([&] { int cfd = ls.accept(); REQUIRE(cfd >= 0); auto r = dbn_record(0x00, 32, 0x99); send_chunk(cfd, r, 0, r.size()); std::this_thread::sleep_for(std::chrono::milliseconds(50)); ::close(cfd); });
    REQUIRE(collect(t, more, 1) >= 0);
    server2.join();
    REQUIRE(more.size() == 1);
    CHECK(more[0][1] == 0x00);
    CHECK(t.connects() == 2);
    CHECK(t.state() == TransportState::UP);
    t.close();
    CHECK(t.poll(b) == -1);
}

TEST_CASE("tcp_stream: zstd magic is fatal, bad record length reconnects") {
    {
        Listener ls;
        TcpStreamTransport t(dbn_cfg(ls.port));
        std::string err;
        REQUIRE(t.open(err));
        std::thread server([&] { int c = ls.accept(); const std::vector<uint8_t> z = {0x28, 0xB5, 0x2F, 0xFD, 0, 0, 0, 0, 1, 2}; send_chunk(c, z, 0, z.size()); std::this_thread::sleep_for(std::chrono::milliseconds(50)); ::close(c); });
        std::vector<std::vector<uint8_t>> got;
        const int rc = collect(t, got, 1, 1000);
        server.join();
        CHECK(rc == -1);
        CHECK(t.last_error().find("zstd") != std::string::npos);
        CHECK(t.state() == TransportState::DOWN);
        REQUIRE(t.state_change_count() == 2);
        CHECK(t.state_change(1).reason == TransportReason::LOCAL_FAILURE);
    }
    {
        Listener ls;
        TcpStreamConfig c = dbn_cfg(ls.port);
        c.prelude = StreamPrelude::NONE;
        TcpStreamTransport t(c);
        std::string err;
        REQUIRE(t.open(err));
        std::thread server([&] { int cfd = ls.accept(); std::vector<uint8_t> bad(64, 0); bad[0] = 0; send_chunk(cfd, bad, 0, bad.size()); std::this_thread::sleep_for(std::chrono::milliseconds(50)); ::close(cfd); });
        std::vector<std::vector<uint8_t>> got;
        const int rc = collect(t, got, 1, 300);
        server.join();
        CHECK(rc == 0);
        CHECK(got.empty());
        CHECK(t.framing_errors() == 1);
    }
}

TEST_CASE("tcp_stream: record_length.add covers a size prefix that excludes itself") {
    std::string err;
    TcpStreamConfig pc;
    REQUIRE(TcpStreamConfig::parse(YAML::Load("{host: 127.0.0.1, port: 9550, record_length: {offset: 0, size: 4, scale: 1, add: 4}}"), pc, err));
    CHECK(pc.len_add == 4);
    CHECK_FALSE(TcpStreamConfig::parse(YAML::Load("{port: 1, record_length: {size: 4, add: 300}}"), pc, err));

    Listener ls;
    TcpStreamConfig c = dbn_cfg(ls.port);
    c.prelude = StreamPrelude::NONE;
    c.len_size = 4; c.len_scale = 1; c.len_add = 4;
    TcpStreamTransport t(c);
    REQUIRE_MESSAGE(t.open(err), err);
    std::vector<uint8_t> stream;
    auto rec = [&](uint32_t body, uint8_t fill) { for (int i = 0; i < 4; ++i) stream.push_back(static_cast<uint8_t>(body >> (8 * i))); stream.insert(stream.end(), body, fill); };
    rec(12, 0xA1); rec(40, 0xB2); rec(7, 0xC3);
    std::thread server([&] { int cfd = ls.accept(); REQUIRE(cfd >= 0); send_chunk(cfd, stream, 0, 9); std::this_thread::sleep_for(std::chrono::milliseconds(15)); send_chunk(cfd, stream, 9, stream.size()); std::this_thread::sleep_for(std::chrono::milliseconds(50)); ::close(cfd); });
    std::vector<std::vector<uint8_t>> got;
    REQUIRE(collect(t, got, 3) >= 0);
    server.join();
    REQUIRE(got.size() == 3);
    CHECK(got[0].size() == 16);
    CHECK(got[1].size() == 44);
    CHECK(got[2].size() == 11);
    CHECK(got[1][0] == 40);
    CHECK(got[1][43] == 0xB2);
    CHECK(t.framing_errors() == 0);
}
