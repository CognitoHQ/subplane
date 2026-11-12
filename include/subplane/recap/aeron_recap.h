#pragma once

// Aeron recap component: requests go out on a publication, answers come back
// on an Aeron response channel, so each agent gets only its own; sections, when
// the service publishes them, arrive on its shared section stream.
//
//   recap:
//     type: aeron
//     aeron_dir: ""                  # media driver directory; empty = Aeron default
//     request:  { channel: "aeron:udp?endpoint=snap-host:40500", stream_id: 1101 }
//     response: { channel: "aeron:udp?control-mode=response|control=snap-host:40501", stream_id: 1102 }
//     sections: { channel: "aeron:udp?endpoint=239.1.2.3:40502|interface=10.0.0.0/8", stream_id: 1103 }  # optional
//     driver_timeout_ms: 10000
//
// The response subscription is added first; its registration id goes on the
// request publication as `response-correlation-id`, which is how the snapshot
// service's response publication finds this agent (Aeron response channels).
// Manager keys (timeout_ms, max_inflight, retry, reply_slabs, ...) live in the
// same block and are read by RecapConfig, not here.

#include "subplane/recap/recap_transport.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

struct aeron_context_stct;
struct aeron_stct;
struct aeron_subscription_stct;
struct aeron_exclusive_publication_stct;
struct aeron_fragment_assembler_stct;
struct aeron_header_stct;

namespace subplane {

struct AeronRecapConfig {
    std::string aeron_dir;
    std::string request_channel;
    int32_t     request_stream = 0;
    std::string response_channel;
    int32_t     response_stream = 0;
    std::string sections_channel;      // empty: the service answers 1:1 only
    int32_t     sections_stream = 0;
    uint64_t    driver_timeout_ms = 10000;

    static bool parse(const YAML::Node& node, AeronRecapConfig& out, std::string& err);
};

class AeronRecap final : public RecapTransport {
public:
    explicit AeronRecap(AeronRecapConfig cfg);
    ~AeronRecap() override;

    bool open(std::string& err) override;
    RecapSendStatus send(const uint8_t* frame, uint32_t len) noexcept override;
    int poll(RecapFragmentSink& sink, int limit) noexcept override;
    void close() noexcept override;
    std::string describe() const override;
    std::string endpoint_key(std::string_view host) const override;
    bool connected() const noexcept override;

    // The request channel as sent to the driver, with response-correlation-id.
    const std::string& request_channel() const noexcept { return request_channel_; }

    static std::unique_ptr<RecapTransport> create(const YAML::Node& cfg, std::string& err);

private:
    static void on_fragment(void* clientd, const uint8_t* buffer, size_t length, aeron_header_stct* header) noexcept;
    static void on_section_fragment(void* clientd, const uint8_t* buffer, size_t length, aeron_header_stct* header) noexcept;
    static void on_client_error(void* clientd, int errcode, const char* message) noexcept;

    AeronRecapConfig cfg_;
    std::string      request_channel_;
    aeron_context_stct*               ctx_ = nullptr;
    aeron_stct*                       client_ = nullptr;
    aeron_subscription_stct*          sub_ = nullptr;
    aeron_subscription_stct*          sections_ = nullptr;
    aeron_exclusive_publication_stct* pub_ = nullptr;
    aeron_fragment_assembler_stct*    assembler_ = nullptr;
    aeron_fragment_assembler_stct*    sections_assembler_ = nullptr;
    RecapFragmentSink*                sink_ = nullptr;   // set during poll()
    std::atomic<uint64_t>             client_errors_{0};  // written on the Aeron conductor thread
};

} // namespace subplane
