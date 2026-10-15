#pragma once

// In-memory recap component: records the requests the manager sends and
// hands queued response fragments to the manager on poll().

#include "subplane/recap/recap_transport.h"

#include <deque>
#include <vector>

namespace subplane::test {

class FakeRecap final : public RecapTransport {
public:
    bool open(std::string&) override { open_ = true; return true; }
    RecapSendStatus send(const RecapRequestWire& req) noexcept override {
        if (send_status != RecapSendStatus::OK) return send_status;
        sent.push_back(req);
        return RecapSendStatus::OK;
    }
    int poll(RecapFragmentSink& sink, int limit) noexcept override {
        int n = 0;
        while (n < limit && !fragments_.empty()) {
            sink.on_fragment(fragments_.front().data(), static_cast<uint32_t>(fragments_.front().size()));
            fragments_.pop_front();
            ++n;
        }
        return n;
    }
    void close() noexcept override { open_ = false; }
    std::string describe() const override { return "fake recap"; }
    bool connected() const noexcept override { return open_; }

    void push(std::vector<uint8_t> fragment) { fragments_.push_back(std::move(fragment)); }

    std::vector<RecapRequestWire> sent;
    RecapSendStatus send_status = RecapSendStatus::OK;

private:
    bool open_ = false;
    std::deque<std::vector<uint8_t>> fragments_;
};

} // namespace subplane::test
