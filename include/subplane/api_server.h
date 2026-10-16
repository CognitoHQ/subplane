#pragma once

// Cold-path HTTP/1.1 server on raw POSIX sockets. Read-only operator surface.
// Thread-safety: start/stop are safe from any thread; handlers run on the
// server thread and only touch the Agent through thread-safe accessors.

#include "subplane/status_types.h"
#include "subplane/types.h"

#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace subplane {

class Agent;

struct HttpRequest {
    StringType method;
    StringType path;
    StringType body;
    std::unordered_map<StringType, StringType> headers;
    std::unordered_map<StringType, StringType> query_params;
};

struct HttpResponse {
    int status_code = 200;
    StringType body;
    std::unordered_map<StringType, StringType> headers;

    HttpResponse() { headers["Content-Type"] = "application/json"; }
};

class ApiServer {
public:
    ApiServer();
    ~ApiServer();

    bool initialize(Agent* agent);
    bool start(const StringType& host, uint16_t port);
    void stop();
    bool is_running() const { return running_.load(); }
    uint16_t get_port() const { return port_; }

    // Exposed for tests.
    HttpRequest parse_request(const StringType& raw_request);
    HttpResponse route_request(const HttpRequest& request);

private:
    Agent* agent_;
    std::atomic<bool> running_;
    std::atomic<bool> should_stop_;
    std::thread server_thread_;
    uint16_t port_;
    int server_socket_;

    void server_loop(const StringType& host, uint16_t port);
    void handle_connection(int client_socket);
    void send_response(int client_socket, const HttpResponse& response);

    HttpResponse handle_health(const HttpRequest& request);
    HttpResponse handle_status(const HttpRequest& request);
    HttpResponse handle_metrics(const HttpRequest& request);
    HttpResponse handle_topology(const HttpRequest& request);
    HttpResponse handle_partitions(const HttpRequest& request);
    HttpResponse handle_sessions(const HttpRequest& request);
    HttpResponse handle_reload(const HttpRequest& request);
    HttpResponse handle_symbols(const HttpRequest& request);
    HttpResponse handle_lvc(const HttpRequest& request);

    static HttpResponse error_response(int code, const StringType& message);
    static StringType json_escape(const StringType& s);
    static StringType serialize_status(const AgentStatus& status);
    static StringType serialize_partitions(const std::vector<PartitionStatus>& parts);
    static StringType serialize_sessions(const std::vector<SessionStatus>& sessions);
};

} // namespace subplane
