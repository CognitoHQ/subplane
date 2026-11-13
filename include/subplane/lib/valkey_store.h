#pragma once
// subplane-lib: the Store in Valkey (or Redis 7+). One Lua script per call, so
// each decision is atomic and timed by the server's clock. Keys share a hash tag
// (the prefix), so the multi-key scripts also work on a cluster.
// One connection, used from one thread (a Runner).

#include "subplane/lib/store.h"

#include <cstdint>
#include <memory>
#include <string>

namespace subplane::lib {

struct ValkeyOptions {
    std::string host = "127.0.0.1";
    int port = 6379;
    std::string unix_path;              // used instead of host:port when set
    uint32_t connect_timeout_ms = 100;
    uint32_t command_timeout_ms = 50;
    uint32_t reconnect_ms = 500;        // between attempts while unreachable
    std::string prefix = "{subplane}:";
};

class ValkeyStore final : public Store {
public:
    explicit ValkeyStore(ValkeyOptions opt);
    ~ValkeyStore() override;

    // The last connection or script error, for logs.
    const std::string& last_error() const noexcept;

    Take take(std::span<const Bucket> path, uint64_t want, uint64_t min) override;
    Claim claim(std::string_view group, std::string_view me, uint32_t ttl_ms) override;
    Renew renew(std::string_view group, std::string_view me, uint64_t epoch, uint32_t ttl_ms) override;
    void release(std::string_view group, std::string_view me, uint64_t epoch) override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace subplane::lib
