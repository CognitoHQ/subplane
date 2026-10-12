#pragma once

// TCP byte-stream transport. Connects to host:port, reassembles the stream
// into whole records using a declared length rule, and hands each record to
// the pipeline as one "packet". Encoding-blind: the length rule and an
// optional stream prelude are configuration, nothing here knows DBN.
//
//   transport:
//     type: tcp_stream
//     host: 127.0.0.1
//     port: 9550
//     record_length: { offset: 0, size: 1, scale: 4 }   # total bytes = field * scale
//     prelude: dbn          # none | dbn ("DBN" u8 ver u32 len, then len bytes; skipped per connection)
//     rcvbuf: 4194304       # reassembly arena bytes (also SO_RCVBUF)
//     batch: 64             # records per poll
//     max_record: 65536
//     reconnect_ms: 1000    # 0 = EOF/error is fatal (poll returns -1)
//     connect_timeout_ms: 2000
//
// Reconnects are silent to the pipeline: poll() returns 0 while disconnected
// and the partition's heartbeat timeout reports DEGRADED as for any quiet bus.

#include "flashtick/transport/transport.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <netinet/in.h>

namespace flashtick {

enum class StreamPrelude : uint8_t { NONE, DBN };

struct TcpStreamConfig {
    std::string   host = "127.0.0.1";
    uint16_t      port = 0;
    uint32_t      len_offset = 0;
    uint8_t       len_size = 1;       // 1, 2 or 4 bytes, little-endian
    uint32_t      len_scale = 1;
    StreamPrelude prelude = StreamPrelude::NONE;
    uint32_t      rcvbuf = 4u << 20;
    int           batch = PacketBatch::kMax;
    uint32_t      max_record = 65536;
    uint32_t      reconnect_ms = 1000;
    uint32_t      connect_timeout_ms = 2000;

    static bool parse(const YAML::Node& node, TcpStreamConfig& out, std::string& err);
};

class TcpStreamTransport final : public Transport {
public:
    explicit TcpStreamTransport(TcpStreamConfig cfg);
    ~TcpStreamTransport() override;

    bool open(std::string& err) override;      // resolves and starts the first connect
    int  poll(PacketBatch& out) noexcept override;
    void close() noexcept override;
    std::string describe() const override;
    int fd() const noexcept override { return fd_; }

    bool connected() const noexcept { return state_ == State::CONNECTED; }
    uint64_t connects() const noexcept { return connects_; }
    uint64_t disconnects() const noexcept { return disconnects_; }
    uint64_t framing_errors() const noexcept { return framing_errors_; }
    const std::string& last_error() const noexcept { return last_error_; }

    static std::unique_ptr<Transport> create(const YAML::Node& cfg, std::string& err);

private:
    enum class State : uint8_t { DISCONNECTED, CONNECTING, CONNECTED, FATAL };

    bool start_connect() noexcept;
    void drop(const char* why) noexcept;        // close socket, schedule reconnect or go FATAL
    bool consume_prelude() noexcept;            // false = need more bytes
    uint32_t record_len_at(const uint8_t* p) const noexcept;

    TcpStreamConfig cfg_;
    sockaddr_in     addr_{};
    int             fd_ = -1;
    State           state_ = State::DISCONNECTED;
    uint64_t        next_connect_ns_ = 0;
    uint64_t        connect_deadline_ns_ = 0;

    // INTENT: one arena allocated in open(); poll() reads into it and cuts
    // records in place. Views point into the arena and are valid until the
    // next poll(), when the unread tail may be compacted to the front.
    std::vector<uint8_t> arena_;
    size_t rd_ = 0, wr_ = 0;
    bool   prelude_pending_ = false;
    size_t prelude_skip_ = 0;

    uint64_t connects_ = 0, disconnects_ = 0, framing_errors_ = 0;
    std::string last_error_;
};

} // namespace flashtick
