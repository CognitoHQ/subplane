#pragma once

// Aeron C client setup shared by the aeron transport and the aeron recap
// component. Cold path only.

#include <cstdint>
#include <string>

#include <aeronc.h>

namespace subplane::aeron_util {

// Initializes and starts a client. On failure everything created is closed,
// ctx and client are left null, and err says what failed.
bool open_client(const std::string& aeron_dir, uint64_t driver_timeout_ms, const char* client_name,
                 aeron_error_handler_t on_error, void* error_clientd,
                 aeron_context_t*& ctx, aeron_t*& client, std::string& err);

// Adds a subscription and waits up to timeout_ms for the driver to confirm it.
aeron_subscription_t* add_subscription(aeron_t* client, const std::string& channel, int32_t stream_id,
                                       aeron_on_available_image_t on_available, void* available_clientd,
                                       aeron_on_unavailable_image_t on_unavailable, void* unavailable_clientd,
                                       uint64_t timeout_ms, std::string& err);

// Adds an exclusive publication and waits up to timeout_ms for the driver to confirm it.
aeron_exclusive_publication_t* add_exclusive_publication(aeron_t* client, const std::string& channel, int32_t stream_id,
                                                         uint64_t timeout_ms, std::string& err);

} // namespace subplane::aeron_util
