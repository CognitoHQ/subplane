#pragma once

// In-memory Transport for pipeline tests: packets pushed from the test are
// returned by poll() in order, batch at a time.

#include "subplane/transport/transport.h"

#include <cstring>
#include <deque>
#include <vector>

namespace subplane::test {

class FakeTransport final : public Transport {
public:
    explicit FakeTransport(int batch = 8, uint32_t mtu = 1500) : batch_(batch), mtu_(mtu) {}

    bool open(std::string&) override {
        arena_.assign(static_cast<size_t>(batch_) * mtu_, 0);
        open_ = true;
        return true;
    }
    int poll(PacketBatch& out) noexcept override {
        out.count = 0;
        if (!open_) return -1;
        int n = 0;
        while (n < batch_ && !queue_.empty()) {
            auto& pkt = queue_.front();
            uint8_t* dst = arena_.data() + static_cast<size_t>(n) * mtu_;
            const uint32_t len = pkt.bytes.size() > mtu_ ? mtu_ : static_cast<uint32_t>(pkt.bytes.size());
            std::memcpy(dst, pkt.bytes.data(), len);
            out.pkts[n] = PacketView{dst, len, pkt.ts};
            queue_.pop_front();
            ++n;
        }
        out.count = n;
        return n;
    }
    void close() noexcept override { open_ = false; }
    std::string describe() const override { return "fake"; }
    int fd() const noexcept override { return -1; }

    void push(std::vector<uint8_t> bytes, uint64_t ts) { queue_.push_back({std::move(bytes), ts}); }
    size_t pending() const { return queue_.size(); }

private:
    struct Pkt { std::vector<uint8_t> bytes; uint64_t ts; };
    int batch_;
    uint32_t mtu_;
    bool open_ = false;
    std::vector<uint8_t> arena_;
    std::deque<Pkt> queue_;
};

} // namespace subplane::test
