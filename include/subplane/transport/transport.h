#pragma once

// Transport component interface: delivers raw packets from a bus partition.
//
// Hot-path contract: exactly ONE virtual call per packet *batch* (poll). The
// batch's PacketViews point into transport-owned memory that stays valid until
// the next poll() on the same transport. No allocation after open().

#include <cstdint>
#include <memory>
#include <string>

#include <yaml-cpp/yaml.h>

namespace subplane {

struct PacketView {
    const uint8_t* data;
    uint32_t       len;
    uint64_t       recv_ts_ns; // CLOCK_REALTIME ns at receive (kernel timestamp if enabled)
};

struct PacketBatch {
    static constexpr int kMax = 64;
    PacketView pkts[kMax];
    int        count = 0;
};

class Transport {
public:
    virtual ~Transport() = default;

    // Cold. Returns false with err set on failure.
    virtual bool open(std::string& err) = 0;

    // Hot. Fills out with up to batch-size packets without blocking.
    // Returns number of packets (0 if none), or -1 on a fatal socket error.
    virtual int poll(PacketBatch& out) noexcept = 0;

    // Cold.
    virtual void close() noexcept = 0;

    // Cold. Human-readable endpoint for logs/status.
    virtual std::string describe() const = 0;

    // Cold. File descriptor for epoll integration (-1 if none).
    virtual int fd() const noexcept = 0;
};

} // namespace subplane
