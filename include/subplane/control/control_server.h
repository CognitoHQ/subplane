#pragma once

// Cold-path control channel server: AF_UNIX SOCK_SEQPACKET, one thread running
// a poll(2) loop over the listen socket and every client socket. Implements
// the protocol in control/control_proto.h on top of SessionManager.
//
// INTENT: this thread never touches pipeline state directly. Every session
// change goes through SessionManager, which posts to the pipeline's SPSC
// command queue and waits for the ack. A slow ack therefore stalls other
// control clients (cold path), never the pipeline.
//
// Session lifetime: a client-owned session is "held" by every connection that
// created or ATTACHed to it. When the last holder disconnects the server calls
// SessionManager::client_disconnected(); SessionManager::service_linger()
// (called every ~100 ms from the poll loop) destroys it after linger. A new
// ATTACH before expiry calls client_reattached() and makes that connection a
// holder.

#include "subplane/control/control_proto.h"
#include "subplane/pipeline/partition.h"
#include "subplane/pipeline/session_manager.h"
#include "subplane/symbol_table.h"

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

namespace subplane {

class ControlServer {
public:
    struct Config {
        std::string socket_path;           // e.g. /run/subplane/<agent>.sock
        std::string fallback_socket_path;  // used when socket_path's directory is missing or not writable
        // Informational (logged at start). Expiry is enforced by
        // SessionManagerConfig::linger_ns; keep the two equal.
        uint64_t    linger_ns = 5'000'000'000ull;
        uint32_t    max_clients = 64;
    };

    // Largest ring a client may request (keeps a misbehaving client from
    // exhausting /dev/shm).
    static constexpr uint64_t kMaxClientRingBytes = 1ull << 30;

    ControlServer(Config cfg, SessionManager& sessions, SymbolTable& symbols,
                  const std::vector<Partition*>& partitions, std::string agent_name, uint64_t epoch);
    ~ControlServer();
    ControlServer(const ControlServer&) = delete;
    ControlServer& operator=(const ControlServer&) = delete;

    // Binds, listens and spawns the server thread.
    bool start(std::string& err);
    // Stops the thread, closes every client (as a disconnect), unlinks the socket.
    void stop();

    const std::string& bound_path() const noexcept { return bound_path_; }
    size_t clients() const noexcept { return n_clients_.load(std::memory_order_relaxed); }

private:
    struct Client {
        int         fd = -1;
        uint32_t    uid = 0;
        int32_t     pid = 0;
        std::vector<std::string> holds;    // client-owned sessions this connection keeps alive
    };

    bool bind_at(const std::string& path, std::string& err);
    void run();
    void accept_clients();
    enum class Rx { SERVED, DRAINED, DROP };
    // Reads and answers at most one request datagram.
    Rx serve(Client& c);
    void drop(size_t idx);
    bool held_by_other(const std::string& name, const Client* except) const;
    void release_everywhere(const std::string& name);

    // Reply helpers. Return false if the client is gone / stuck.
    bool send_dgram(Client& c, const void* data, size_t len);
    bool reply(Client& c, const CtlHeader& req, CtlStatus st, const void* body, size_t len);
    bool reply_err(Client& c, const CtlHeader& req, CtlStatus st, const std::string& msg);

    bool op_hello(Client& c, const CtlHeader& h);
    bool op_attach(Client& c, const CtlHeader& h, const uint8_t* body);
    bool op_create(Client& c, const CtlHeader& h, const uint8_t* body);
    bool op_set_filter(Client& c, const CtlHeader& h, const uint8_t* body);
    bool op_destroy(Client& c, const CtlHeader& h, const uint8_t* body);
    bool op_snapshot(Client& c, const CtlHeader& h, const uint8_t* body);
    bool op_symbols(Client& c, const CtlHeader& h, const uint8_t* body);
    bool op_ping(Client& c, const CtlHeader& h, const uint8_t* body);

    // Decodes a CREATE/SET_FILTER body into a SessionSpec (owner = client).
    bool to_spec(const CtlHeader& h, const uint8_t* body, bool check_ring, SessionSpec& out,
                 std::string& err) const;
    bool fill_attach(const std::string& name, CtlAttachResp& out, std::string& err) const;

    Config                  cfg_;
    SessionManager&         sessions_;
    SymbolTable&            symbols_;
    std::vector<Partition*> partitions_;
    std::string             agent_name_;
    uint64_t                epoch_;

    std::string             bound_path_;
    int                     listen_fd_ = -1;
    int                     wake_fd_ = -1;   // eventfd; stop() writes it to break poll()
    std::thread             thread_;
    std::atomic<bool>       stop_{false};
    std::atomic<size_t>     n_clients_{0};
    std::vector<Client>     clients_;        // server thread only
    std::vector<uint8_t>    rx_;             // server thread only
};

} // namespace subplane
