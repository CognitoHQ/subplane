#pragma once

// In-memory recap component: records the frames the manager sends and hands
// queued response fragments to the manager on poll().

#include "subplane/recap/recap_transport.h"

#include <deque>
#include <utility>
#include <vector>

namespace subplane::test {

class FakeRecap final : public RecapTransport {
public:
    bool open(std::string&) override { open_ = true; return true; }
    RecapSendStatus send(const uint8_t* frame, uint32_t len) noexcept override {
        if (send_status != RecapSendStatus::OK) return send_status;
        sent.emplace_back(frame, frame + len);
        return RecapSendStatus::OK;
    }
    int poll(RecapFragmentSink& sink, int limit) noexcept override {
        int n = 0;
        while (n < limit && !fragments_.empty()) {
            sink.on_fragment(fragments_.front().first.data(), static_cast<uint32_t>(fragments_.front().first.size()), fragments_.front().second);
            fragments_.pop_front();
            ++n;
        }
        return n;
    }
    void close() noexcept override { open_ = false; }
    std::string describe() const override { return "fake recap"; }
    bool connected() const noexcept override { return open_ && link_up; }

    void push(std::vector<uint8_t> fragment, RecapSource src = RecapSource::RESPONSE) { fragments_.emplace_back(std::move(fragment), src); }
    void push_section(std::vector<uint8_t> fragment) { push(std::move(fragment), RecapSource::SECTIONS); }

    std::vector<std::vector<uint8_t>> sent;   // one entry per frame
    RecapSendStatus send_status = RecapSendStatus::OK;
    bool link_up = true;

private:
    bool open_ = false;
    std::deque<std::pair<std::vector<uint8_t>, RecapSource>> fragments_;
};

} // namespace subplane::test
