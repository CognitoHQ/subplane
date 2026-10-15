#include "subplane/recap/aeron_recap.h"

#include "../transport/aeron_util.h"

#include <aeronc.h>

namespace subplane {

bool AeronRecapConfig::parse(const YAML::Node& node, AeronRecapConfig& out, std::string& err) {
    try {
        out = AeronRecapConfig{};
        out.aeron_dir = node["aeron_dir"].as<std::string>("");
        out.driver_timeout_ms = node["driver_timeout_ms"].as<uint64_t>(10000u);
        auto endpoint = [&](const char* key, std::string& channel, int32_t& stream) {
            const auto n = node[key];
            if (!n || !n.IsMap() || !n["channel"] || !n["stream_id"]) {
                err = std::string("recap.") + key + " must be {channel, stream_id}";
                return false;
            }
            channel = n["channel"].as<std::string>();
            stream = n["stream_id"].as<int32_t>();
            return true;
        };
        if (!endpoint("request", out.request_channel, out.request_stream)) return false;
        if (!endpoint("response", out.response_channel, out.response_stream)) return false;
    } catch (const YAML::Exception& e) {
        err = std::string("recap config: ") + e.what();
        return false;
    }
    if (out.response_channel.find("control-mode=response") == std::string::npos) {
        err = "recap.response.channel must be an Aeron response channel (control-mode=response|control=<service>:<port>)";
        return false;
    }
    if (out.request_channel.find("response-correlation-id") != std::string::npos) {
        err = "recap.request.channel must not set response-correlation-id; the agent adds it";
        return false;
    }
    return true;
}

std::unique_ptr<RecapTransport> AeronRecap::create(const YAML::Node& cfg, std::string& err) {
    AeronRecapConfig c;
    if (!AeronRecapConfig::parse(cfg, c, err)) return nullptr;
    return std::make_unique<AeronRecap>(std::move(c));
}

AeronRecap::AeronRecap(AeronRecapConfig cfg) : cfg_(std::move(cfg)) {}
AeronRecap::~AeronRecap() { close(); }

bool AeronRecap::open(std::string& err) {
    if (client_) { err = "aeron recap already open"; return false; }
    if (!aeron_util::open_client(cfg_.aeron_dir, cfg_.driver_timeout_ms, "subplaned-recap", &AeronRecap::on_client_error, this,
                                 ctx_, client_, err)) { close(); return false; }
    sub_ = aeron_util::add_subscription(client_, cfg_.response_channel, cfg_.response_stream,
                                        nullptr, nullptr, nullptr, nullptr, cfg_.driver_timeout_ms, err);
    if (!sub_) { close(); return false; }
    aeron_subscription_constants_t sc{};
    if (aeron_subscription_constants(sub_, &sc) < 0) { err = std::string("aeron subscription constants: ") + aeron_errmsg(); close(); return false; }
    // INTENT: the service's response publication is matched to this
    // subscription through this id; without it replies have nowhere to go.
    request_channel_ = cfg_.request_channel;
    request_channel_ += request_channel_.find('?') == std::string::npos ? '?' : '|';
    request_channel_ += "response-correlation-id=" + std::to_string(sc.registration_id);
    pub_ = aeron_util::add_exclusive_publication(client_, request_channel_, cfg_.request_stream, cfg_.driver_timeout_ms, err);
    if (!pub_) { close(); return false; }
    if (aeron_fragment_assembler_create(&assembler_, &AeronRecap::on_fragment, this) < 0) {
        err = std::string("aeron fragment assembler: ") + aeron_errmsg(); close(); return false;
    }
    return true;
}

RecapSendStatus AeronRecap::send(const RecapRequestWire& req) noexcept {
    if (!pub_) return RecapSendStatus::FAILED;
    const int64_t rc = aeron_exclusive_publication_offer(pub_, reinterpret_cast<const uint8_t*>(&req), sizeof(req), nullptr, nullptr);
    if (rc >= 0) return RecapSendStatus::OK;
    switch (rc) {
        case AERON_PUBLICATION_NOT_CONNECTED: return RecapSendStatus::NOT_CONNECTED;
        case AERON_PUBLICATION_BACK_PRESSURED:
        case AERON_PUBLICATION_ADMIN_ACTION:  return RecapSendStatus::BACK_PRESSURED;
        default:                              return RecapSendStatus::FAILED;
    }
}

void AeronRecap::on_fragment(void* clientd, const uint8_t* buffer, size_t length, aeron_header_stct*) noexcept {
    auto* self = static_cast<AeronRecap*>(clientd);
    if (self->sink_) self->sink_->on_fragment(buffer, static_cast<uint32_t>(length));
}

void AeronRecap::on_client_error(void* clientd, int, const char*) noexcept {
    static_cast<AeronRecap*>(clientd)->client_errors_.fetch_add(1, std::memory_order_relaxed);
}

int AeronRecap::poll(RecapFragmentSink& sink, int limit) noexcept {
    if (!sub_) return 0;
    sink_ = &sink;
    const int n = aeron_subscription_poll(sub_, aeron_fragment_assembler_handler, assembler_, static_cast<size_t>(limit));
    sink_ = nullptr;
    return n < 0 ? 0 : n;
}

bool AeronRecap::connected() const noexcept {
    return pub_ && aeron_exclusive_publication_is_connected(pub_);
}

void AeronRecap::close() noexcept {
    if (assembler_) { aeron_fragment_assembler_delete(assembler_); assembler_ = nullptr; }
    if (pub_) { aeron_exclusive_publication_close(pub_, nullptr, nullptr); pub_ = nullptr; }
    if (sub_) { aeron_subscription_close(sub_, nullptr, nullptr); sub_ = nullptr; }
    if (client_) { aeron_close(client_); client_ = nullptr; }
    if (ctx_) { aeron_context_close(ctx_); ctx_ = nullptr; }
}

std::string AeronRecap::describe() const {
    std::string s = "aeron recap request " + cfg_.request_channel + " stream " + std::to_string(cfg_.request_stream) +
                    ", response stream " + std::to_string(cfg_.response_stream);
    if (pub_) s += connected() ? " connected" : " not connected";
    return s;
}

} // namespace subplane
