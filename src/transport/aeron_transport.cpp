#include "subplane/transport/aeron_transport.h"

#include "subplane/core/counters.h"
#include "subplane/observability/endpoint_key.h"

#include "aeron_util.h"

#include "subplane/core/clock.h"

#include <aeronc.h>
#include <aeronmd.h>

#include <cstring>
#include <sched.h>

namespace subplane {

bool AeronTransportConfig::parse(const YAML::Node& node, AeronTransportConfig& out, std::string& err) {
    try {
        out.channel = node["channel"].as<std::string>("");
        if (out.channel.empty()) { err = "transport.channel is required (e.g. aeron:ipc)"; return false; }
        if (!node["stream_id"]) { err = "transport.stream_id is required"; return false; }
        out.stream_id = node["stream_id"].as<int32_t>();
        out.aeron_dir = node["aeron_dir"].as<std::string>("");
        out.batch = node["batch"].as<int>(32);
        out.max_message = node["max_message"].as<uint32_t>(65536u);
        out.driver_timeout_ms = node["driver_timeout_ms"].as<uint64_t>(10000u);
        if (const auto d = node["driver"]) {
            if (!d.IsMap()) { err = "transport.driver must be a map"; return false; }
            out.embedded_driver = d["embedded"].as<bool>(false);
            out.driver_threading = d["threading_mode"].as<std::string>("shared");
            out.driver_dir_delete_on_start = d["dir_delete_on_start"].as<bool>(true);
        }
    } catch (const YAML::Exception& e) {
        err = std::string("transport config: ") + e.what();
        return false;
    }
    if (out.batch < 1 || out.batch > PacketBatch::kMax) {
        err = "transport.batch must be 1.." + std::to_string(PacketBatch::kMax);
        return false;
    }
    if (out.max_message < 64 || out.max_message > (16u << 20)) {
        err = "transport.max_message must be 64..16777216";
        return false;
    }
    if (out.driver_threading != "shared" && out.driver_threading != "shared_network" && out.driver_threading != "dedicated") {
        err = "transport.driver.threading_mode must be shared, shared_network or dedicated";
        return false;
    }
    return true;
}

std::unique_ptr<Transport> AeronTransport::create(const YAML::Node& cfg, std::string& err) {
    AeronTransportConfig c;
    if (!AeronTransportConfig::parse(cfg, c, err)) return nullptr;
    return std::make_unique<AeronTransport>(std::move(c));
}

AeronTransport::AeronTransport(AeronTransportConfig cfg) : cfg_(std::move(cfg)) {}
AeronTransport::~AeronTransport() { close(); }

bool AeronTransport::start_driver(std::string& err) {
    if (aeron_driver_context_init(&driver_ctx_) < 0) { err = std::string("aeron driver context: ") + aeron_errmsg(); return false; }
    if (!cfg_.aeron_dir.empty() && aeron_driver_context_set_dir(driver_ctx_, cfg_.aeron_dir.c_str()) < 0) {
        err = std::string("aeron driver dir: ") + aeron_errmsg(); return false;
    }
    aeron_threading_mode_t mode = AERON_THREADING_MODE_SHARED;
    if (cfg_.driver_threading == "shared_network") mode = AERON_THREADING_MODE_SHARED_NETWORK;
    else if (cfg_.driver_threading == "dedicated") mode = AERON_THREADING_MODE_DEDICATED;
    aeron_driver_context_set_threading_mode(driver_ctx_, mode);
    aeron_driver_context_set_dir_delete_on_start(driver_ctx_, cfg_.driver_dir_delete_on_start);
    aeron_driver_context_set_dir_delete_on_shutdown(driver_ctx_, true);
    if (aeron_driver_init(&driver_, driver_ctx_) < 0) { err = std::string("aeron driver init: ") + aeron_errmsg(); return false; }
    // INTENT: manual_main_loop=false gives the driver its own threads. The
    // pipeline thread must never run the media driver's duty cycle.
    if (aeron_driver_start(driver_, false) < 0) { err = std::string("aeron driver start: ") + aeron_errmsg(); return false; }
    return true;
}

bool AeronTransport::open(std::string& err) {
    if (client_) { err = "aeron transport already open"; return false; }
    if (cfg_.embedded_driver && !start_driver(err)) { close(); return false; }

    arena_.assign(static_cast<size_t>(cfg_.batch) * cfg_.max_message, 0);

    if (!aeron_util::open_client(cfg_.aeron_dir, cfg_.driver_timeout_ms, "subplaned", &AeronTransport::on_client_error, this,
                                 ctx_, client_, err)) { close(); return false; }
    sub_ = aeron_util::add_subscription(client_, cfg_.channel, cfg_.stream_id,
                                        &AeronTransport::on_available_image, this,
                                        &AeronTransport::on_unavailable_image, this, cfg_.driver_timeout_ms, err);
    if (!sub_) { close(); return false; }
    if (aeron_fragment_assembler_create(&assembler_, &AeronTransport::on_fragment, this) < 0) {
        err = std::string("aeron fragment assembler: ") + aeron_errmsg(); close(); return false;
    }
    return true;
}

void AeronTransport::on_fragment(void* clientd, const uint8_t* buffer, size_t length, aeron_header_stct*) noexcept {
    auto* self = static_cast<AeronTransport*>(clientd);
    inc(self->fragments_);
    if (length > self->cfg_.max_message) { inc(self->oversize_); return; }
    PacketBatch& b = *self->cur_;
    uint8_t* slot = self->arena_.data() + static_cast<size_t>(b.count) * self->cfg_.max_message;
    std::memcpy(slot, buffer, length);
    PacketView& p = b.pkts[b.count++];
    p.data = slot;
    p.len = static_cast<uint32_t>(length);
    p.recv_ts_ns = self->cur_ts_;
}

int AeronTransport::poll(PacketBatch& out) noexcept {
    out.count = 0;
    if (client_dead_.load(std::memory_order_acquire)) {
        // The client closed itself; its subscription must not be touched.
        report_down(TransportReason::LOCAL_FAILURE);
        return -1;
    }
    poll_state();
    cur_ = &out;
    cur_ts_ = now_ns();
    // INTENT: the fragment limit equals the arena slot count, so the handler can
    // never run past the arena; Aeron delivers at most `batch` fragments per poll.
    const int n = aeron_subscription_poll(sub_, aeron_fragment_assembler_handler, assembler_,
                                          static_cast<size_t>(cfg_.batch));
    cur_ = nullptr;
    if (n < 0) return -1;
    return out.count;
}

// INTENT: up/down is decided on the pipeline thread, before the fragments
// are read, so a TRANSPORT_UP lands ahead of the records it announces. The
// image count is the truth for up/down (an image is in the subscription
// before its handler runs). The handler only supplies the newest image's
// session id: a new id means a new publisher instance, reported as
// UP/PUBLISHER_RESTART even when the old image has not timed out yet. When a
// poll sees the new image before its handler ran, that UP/PUBLISHER_RESTART
// follows on the next poll, after a plain UP and possibly a few records.
void AeronTransport::poll_state() noexcept {
    const uint64_t avail = images_available_.load(std::memory_order_acquire);
    if (avail != seen_available_) {
        seen_available_ = avail;
        const int32_t sid = latest_session_.load(std::memory_order_relaxed);
        const bool restart = have_session_ && sid != session_;
        session_ = sid;
        have_session_ = true;
        report_up(restart ? TransportReason::PUBLISHER_RESTART : TransportReason::NONE);
    }
    if (aeron_subscription_image_count(sub_) > 0) report_up();
    else report_down(TransportReason::SOURCE_LOST);
}

void AeronTransport::on_available_image(void* clientd, aeron_subscription_stct*, aeron_image_stct* image) noexcept {
    auto* self = static_cast<AeronTransport*>(clientd);
    aeron_image_constants_t c{};
    if (aeron_image_constants(image, &c) == 0) self->latest_session_.store(c.session_id, std::memory_order_relaxed);
    self->images_available_.fetch_add(1, std::memory_order_release);
}
void AeronTransport::on_unavailable_image(void* clientd, aeron_subscription_stct*, aeron_image_stct*) noexcept {
    static_cast<AeronTransport*>(clientd)->images_unavailable_.fetch_add(1, std::memory_order_relaxed);
}
void AeronTransport::on_client_error(void* clientd, int errcode, const char* message) noexcept {
    auto* self = static_cast<AeronTransport*>(clientd);
    self->client_errors_.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lk(self->error_mu_);
        self->last_error_ = message ? message : "";
    }
    // INTENT: these three mean the client has given up and closed itself;
    // every other error is reported and the client carries on.
    if (errcode == AERON_CLIENT_ERROR_DRIVER_TIMEOUT || errcode == AERON_CLIENT_ERROR_CLIENT_TIMEOUT ||
        errcode == AERON_CLIENT_ERROR_CONDUCTOR_SERVICE_TIMEOUT) {
        self->client_dead_.store(true, std::memory_order_release);
    }
}

std::string AeronTransport::last_error() const {
    std::lock_guard<std::mutex> lk(error_mu_);
    return last_error_;
}

bool AeronTransport::connected() const noexcept {
    return sub_ && aeron_subscription_is_connected(sub_);
}

void AeronTransport::close() noexcept {
    if (assembler_) { aeron_fragment_assembler_delete(assembler_); assembler_ = nullptr; }
    if (sub_) { aeron_subscription_close(sub_, nullptr, nullptr); sub_ = nullptr; }
    if (client_) { aeron_close(client_); client_ = nullptr; }
    if (ctx_) { aeron_context_close(ctx_); ctx_ = nullptr; }
    if (driver_) { aeron_driver_close(driver_); driver_ = nullptr; }
    if (driver_ctx_) { aeron_driver_context_close(driver_ctx_); driver_ctx_ = nullptr; }
}

std::string AeronTransport::endpoint_key(std::string_view host) const {
    return observability::aeron_key(cfg_.channel, cfg_.stream_id, aeron_util::resolved_dir(cfg_.aeron_dir), host);
}

std::string AeronTransport::publisher_json() const {
    // The newest image's session id: a new one means a new publisher instance.
    if (images_available() == images_unavailable()) return {};
    return "\"aeron_session\":" + std::to_string(latest_session_.load(std::memory_order_relaxed));
}

void AeronTransport::counters(const std::function<void(std::string_view, uint64_t)>& out) const {
    out("fragments", fragments());
    out("oversize", oversize());
    out("images_available", images_available());
    out("images_unavailable", images_unavailable());
    out("client_errors", client_errors());
}

std::string AeronTransport::describe() const {
    std::string s = "aeron " + cfg_.channel + " stream " + std::to_string(cfg_.stream_id);
    if (cfg_.embedded_driver) s += " (embedded driver)";
    if (sub_) s += connected() ? " connected" : " not connected";
    return s;
}

} // namespace subplane
