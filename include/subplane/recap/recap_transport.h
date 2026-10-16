#pragma once

// Recap component interface: carries SPP frames (docs/protocol/
// subplane-protocol.md, section 4: u32-length-prefixed messages) to a snapshot
// service and response fragments back. Compiled in and selected by
// `recap.type` in a partition's YAML, like a transport. Runs on the recap
// manager's cold thread only, so virtual calls are fine here. The component
// moves bytes; it never interprets them.

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include <yaml-cpp/yaml.h>

namespace subplane {

enum class RecapSendStatus : uint8_t { OK, BACK_PRESSURED, NOT_CONNECTED, FAILED };

// Receives response fragments during poll(). A fragment is one unit as the
// service sent it (for Aeron, one reassembled offer); the manager splits it
// into messages (SPP length prefixes, or the partition's framing in
// `reply: stream_markers` mode).
class RecapFragmentSink {
public:
    virtual ~RecapFragmentSink() = default;
    virtual void on_fragment(const uint8_t* data, uint32_t len) noexcept = 0;
};

class RecapTransport {
public:
    virtual ~RecapTransport() = default;

    // Cold. False with err set on failure.
    virtual bool open(std::string& err) = 0;
    // One frame: for Aeron, one offer.
    virtual RecapSendStatus send(const uint8_t* frame, uint32_t len) noexcept = 0;
    // Delivers up to `limit` response fragments to the sink; returns how many.
    virtual int poll(RecapFragmentSink& sink, int limit) noexcept = 0;
    virtual void close() noexcept = 0;
    virtual std::string describe() const = 0;
    // Cold. Canonical key of the request stream to the snapshot service
    // (observability/endpoint_key.h); empty if it has none.
    virtual std::string endpoint_key(std::string_view host) const { (void)host; return {}; }
    virtual bool connected() const noexcept = 0;
};

const char* recap_send_status_name(RecapSendStatus s) noexcept;

} // namespace subplane
