#include "aeron_util.h"

#include "subplane/core/clock.h"

#include <sched.h>

namespace subplane::aeron_util {

std::string resolved_dir(const std::string& aeron_dir) {
    if (!aeron_dir.empty()) return aeron_dir;
    char buf[512] = {};
    return aeron_default_path(buf, sizeof buf) >= 0 ? std::string(buf) : std::string();
}

bool open_client(const std::string& aeron_dir, uint64_t driver_timeout_ms, const char* client_name,
                 aeron_error_handler_t on_error, void* error_clientd,
                 aeron_context_t*& ctx, aeron_t*& client, std::string& err) {
    ctx = nullptr;
    client = nullptr;
    auto fail = [&](const char* what) {
        err = std::string(what) + aeron_errmsg();
        if (client) { aeron_close(client); client = nullptr; }
        if (ctx) { aeron_context_close(ctx); ctx = nullptr; }
        return false;
    };
    if (aeron_context_init(&ctx) < 0) return fail("aeron context: ");
    if (!aeron_dir.empty() && aeron_context_set_dir(ctx, aeron_dir.c_str()) < 0) return fail("aeron dir: ");
    aeron_context_set_driver_timeout_ms(ctx, driver_timeout_ms);
    aeron_context_set_client_name(ctx, client_name);
    if (on_error) aeron_context_set_error_handler(ctx, on_error, error_clientd);
    if (aeron_init(&client, ctx) < 0) return fail("aeron init (is the media driver running?): ");
    if (aeron_start(client) < 0) return fail("aeron start: ");
    return true;
}

aeron_subscription_t* add_subscription(aeron_t* client, const std::string& channel, int32_t stream_id,
                                       aeron_on_available_image_t on_available, void* available_clientd,
                                       aeron_on_unavailable_image_t on_unavailable, void* unavailable_clientd,
                                       uint64_t timeout_ms, std::string& err) {
    aeron_async_add_subscription_t* async = nullptr;
    if (aeron_async_add_subscription(&async, client, channel.c_str(), stream_id, on_available, available_clientd,
                                     on_unavailable, unavailable_clientd) < 0) {
        err = std::string("aeron add subscription: ") + aeron_errmsg();
        return nullptr;
    }
    const uint64_t deadline = now_mono_ns() + timeout_ms * 1000000ull;
    aeron_subscription_t* sub = nullptr;
    for (;;) {
        const int rc = aeron_async_add_subscription_poll(&sub, async);
        if (rc < 0) { err = std::string("aeron subscription: ") + aeron_errmsg(); return nullptr; }
        if (rc > 0 && sub) return sub;
        if (now_mono_ns() > deadline) { err = "aeron subscription: timed out waiting for the media driver"; return nullptr; }
        sched_yield();
    }
}

aeron_exclusive_publication_t* add_exclusive_publication(aeron_t* client, const std::string& channel, int32_t stream_id,
                                                         uint64_t timeout_ms, std::string& err) {
    aeron_async_add_exclusive_publication_t* async = nullptr;
    if (aeron_async_add_exclusive_publication(&async, client, channel.c_str(), stream_id) < 0) {
        err = std::string("aeron add publication: ") + aeron_errmsg();
        return nullptr;
    }
    const uint64_t deadline = now_mono_ns() + timeout_ms * 1000000ull;
    aeron_exclusive_publication_t* pub = nullptr;
    for (;;) {
        const int rc = aeron_async_add_exclusive_publication_poll(&pub, async);
        if (rc < 0) { err = std::string("aeron publication: ") + aeron_errmsg(); return nullptr; }
        if (rc > 0 && pub) return pub;
        if (now_mono_ns() > deadline) { err = "aeron publication: timed out waiting for the media driver"; return nullptr; }
        sched_yield();
    }
}

} // namespace subplane::aeron_util
