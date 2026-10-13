#pragma once

// Aeron ingest transport. Subscribes to one Aeron stream (IPC, UDP unicast or
// multicast) through the Aeron C client and hands each reassembled message to
// the pipeline as one packet. Encoding-blind: the framing block says where the
// sequence lives; `length: packet` (or the schema default with one message per
// publication offer) is the usual pairing.
//
//   transport:
//     type: aeron
//     channel: "aeron:ipc"              # or aeron:udp?endpoint=0.0.0.0:40456 | aeron:udp?endpoint=239.1.1.1:40456|interface=...
//     stream_id: 1001
//     aeron_dir: ""                     # media driver directory; empty = Aeron default (/dev/shm/aeron-<user>)
//     batch: 32                         # fragments per poll, <= 64
//     max_message: 65536                # bytes per arena slot; larger messages are counted and dropped
//     driver_timeout_ms: 10000          # how long the client waits for a live driver on open
//     driver:                           # optional in-process media driver (dev, tests, single-box setups)
//       embedded: false
//       threading_mode: shared          # shared | shared_network | dedicated
//       dir_delete_on_start: true
//
// The media driver is normally a separate process (`aeronmd`); `driver.embedded`
// runs one inside the agent on its own threads, never on the pipeline thread.
// The C client's conductor also runs on its own thread; poll() only reads the
// image log buffers (lock-free) and copies fragments into the transport arena.

#include "flashtick/transport/transport.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

struct aeron_context_stct;
struct aeron_stct;
struct aeron_subscription_stct;
struct aeron_fragment_assembler_stct;
struct aeron_image_stct;
struct aeron_driver_context_stct;
struct aeron_driver_stct;
struct aeron_header_stct;

namespace flashtick {

struct AeronTransportConfig {
    std::string channel;
    int32_t     stream_id = 0;
    std::string aeron_dir;                 // empty = Aeron default
    int         batch = 32;                // fragment limit per poll
    uint32_t    max_message = 65536;       // arena slot bytes
    uint64_t    driver_timeout_ms = 10000;
    bool        embedded_driver = false;
    std::string driver_threading = "shared";
    bool        driver_dir_delete_on_start = true;

    static bool parse(const YAML::Node& node, AeronTransportConfig& out, std::string& err);
};

class AeronTransport final : public Transport {
public:
    explicit AeronTransport(AeronTransportConfig cfg);
    ~AeronTransport() override;

    bool open(std::string& err) override;
    int  poll(PacketBatch& out) noexcept override;
    void close() noexcept override;
    std::string describe() const override;
    int fd() const noexcept override { return -1; }

    bool connected() const noexcept;
    uint64_t fragments() const noexcept { return fragments_; }
    uint64_t oversize() const noexcept { return oversize_; }
    uint64_t images_available() const noexcept { return images_available_; }
    uint64_t images_unavailable() const noexcept { return images_unavailable_; }
    uint64_t client_errors() const noexcept { return client_errors_; }
    const std::string& last_error() const noexcept { return last_error_; }

    static std::unique_ptr<Transport> create(const YAML::Node& cfg, std::string& err);

private:
    static void on_fragment(void* clientd, const uint8_t* buffer, size_t length, aeron_header_stct* header) noexcept;
    static void on_available_image(void* clientd, aeron_subscription_stct* sub, aeron_image_stct* image) noexcept;
    static void on_unavailable_image(void* clientd, aeron_subscription_stct* sub, aeron_image_stct* image) noexcept;
    static void on_client_error(void* clientd, int errcode, const char* message) noexcept;
    bool start_driver(std::string& err);

    AeronTransportConfig cfg_;
    aeron_driver_context_stct* driver_ctx_ = nullptr;
    aeron_driver_stct*         driver_ = nullptr;
    aeron_context_stct*        ctx_ = nullptr;
    aeron_stct*                client_ = nullptr;
    aeron_subscription_stct*   sub_ = nullptr;
    aeron_fragment_assembler_stct* assembler_ = nullptr;

    // INTENT: one arena of batch * max_message bytes allocated in open(). The
    // fragment handler copies each message into the next slot because Aeron's
    // log-buffer bytes are only guaranteed while the handler runs; a view that
    // outlives poll() (the pipeline reads the batch after poll returns) must own
    // its bytes. One memcpy per message; measured, not assumed.
    std::vector<uint8_t> arena_;
    PacketBatch* cur_ = nullptr;     // batch being filled during poll()
    uint64_t     cur_ts_ = 0;        // one clock read per poll

    uint64_t fragments_ = 0, oversize_ = 0;
    uint64_t images_available_ = 0, images_unavailable_ = 0, client_errors_ = 0;
    std::string last_error_;
};

} // namespace flashtick
