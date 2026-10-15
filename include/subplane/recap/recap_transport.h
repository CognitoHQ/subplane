#pragma once

// Recap component interface: carries recap requests to a snapshot service and
// response fragments back. Compiled in and selected by `recap.type` in a
// partition's YAML, like a transport. Runs on the recap manager's cold thread
// only, so virtual calls are fine here.

#include "subplane/recap/recap_request.h"

#include <cstdint>
#include <memory>
#include <string>

#include <yaml-cpp/yaml.h>

namespace subplane {

enum class RecapSendStatus : uint8_t { OK, BACK_PRESSURED, NOT_CONNECTED, FAILED };

// Receives response fragments during poll(). A fragment is one unit as the
// service sent it (for Aeron, one reassembled message); the manager splits it
// into messages with the partition's framing.
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
    virtual RecapSendStatus send(const RecapRequestWire& req) noexcept = 0;
    // Delivers up to `limit` response fragments to the sink; returns how many.
    virtual int poll(RecapFragmentSink& sink, int limit) noexcept = 0;
    virtual void close() noexcept = 0;
    virtual std::string describe() const = 0;
    virtual bool connected() const noexcept = 0;
};

const char* recap_send_status_name(RecapSendStatus s) noexcept;

} // namespace subplane
