#include "subplane/api_server.h"

#include "subplane/agent.h"
#include "subplane/logging/log_registry.h"
#include "subplane/metrics/metrics_manager.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <netinet/in.h>
#include <sstream>
#include <sys/socket.h>
#include <unistd.h>

namespace subplane {

ApiServer::ApiServer()
    : agent_(nullptr), running_(false), should_stop_(false), port_(0), server_socket_(-1) {}

ApiServer::~ApiServer() {
    stop();
}

bool ApiServer::initialize(Agent* agent) {
    if (!agent) {
        return false;
    }
    agent_ = agent;
    return true;
}

bool ApiServer::start(const StringType& host, uint16_t port) {
    if (running_.load()) {
        return false;
    }
    port_ = port;
    should_stop_.store(false);

    // Bind synchronously so start() can report failure.
    server_socket_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket_ < 0) {
        LogRegistry::instance().error("API_SERVER", "SOCKET_FAILED", strerror(errno));
        return false;
    }
    int opt = 1;
    setsockopt(server_socket_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    if (host == "0.0.0.0" || host == "*") {
        address.sin_addr.s_addr = INADDR_ANY;
    } else if (inet_pton(AF_INET, host.c_str(), &address.sin_addr) != 1) {
        LogRegistry::instance().error("API_SERVER", "BAD_HOST", host);
        close(server_socket_);
        server_socket_ = -1;
        return false;
    }
    if (bind(server_socket_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        LogRegistry::instance().error("API_SERVER", "BIND_FAILED", strerror(errno));
        close(server_socket_);
        server_socket_ = -1;
        return false;
    }
    if (listen(server_socket_, 64) < 0) {
        LogRegistry::instance().error("API_SERVER", "LISTEN_FAILED", strerror(errno));
        close(server_socket_);
        server_socket_ = -1;
        return false;
    }

    running_.store(true);
    server_thread_ = std::thread(&ApiServer::server_loop, this, host, port);
    LogRegistry::instance().info("API_SERVER", "STARTED", host + ":" + std::to_string(port));
    return true;
}

void ApiServer::stop() {
    if (!running_.load()) {
        return;
    }
    should_stop_.store(true);
    if (server_socket_ >= 0) {
        shutdown(server_socket_, SHUT_RDWR);
        close(server_socket_);
        server_socket_ = -1;
    }
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
    running_.store(false);
    LogRegistry::instance().info("API_SERVER", "STOPPED", "");
}

void ApiServer::server_loop(const StringType&, uint16_t) {
    while (!should_stop_.load()) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_socket = accept(server_socket_, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
        if (client_socket < 0) {
            if (should_stop_.load()) {
                break;
            }
            continue;
        }
        handle_connection(client_socket);
        close(client_socket);
    }
}

void ApiServer::handle_connection(int client_socket) {
    char buffer[16384];
    ssize_t bytes_read = read(client_socket, buffer, sizeof(buffer) - 1);
    if (bytes_read <= 0) {
        return;
    }
    buffer[bytes_read] = '\0';
    HttpRequest request = parse_request(StringType(buffer, static_cast<size_t>(bytes_read)));
    HttpResponse response = route_request(request);
    send_response(client_socket, response);
}

HttpRequest ApiServer::parse_request(const StringType& raw_request) {
    HttpRequest request;
    std::istringstream stream(raw_request);
    StringType line;

    if (std::getline(stream, line)) {
        std::istringstream request_line(line);
        request_line >> request.method >> request.path;
    }
    while (std::getline(stream, line) && line != "\r" && !line.empty()) {
        size_t colon = line.find(':');
        if (colon == StringType::npos) {
            continue;
        }
        StringType key = line.substr(0, colon);
        StringType value = line.substr(colon + 1);
        auto trim = [](StringType& s) {
            s.erase(0, s.find_first_not_of(" \t\r\n"));
            s.erase(s.find_last_not_of(" \t\r\n") + 1);
        };
        trim(key);
        trim(value);
        request.headers[key] = value;
    }
    StringType body;
    while (std::getline(stream, line)) {
        body += line + "\n";
    }
    if (!body.empty()) {
        body.pop_back();
    }
    request.body = body;

    size_t q = request.path.find('?');
    if (q != StringType::npos) {
        StringType query = request.path.substr(q + 1);
        request.path = request.path.substr(0, q);
        std::istringstream qs(query);
        StringType param;
        while (std::getline(qs, param, '&')) {
            size_t eq = param.find('=');
            if (eq != StringType::npos) {
                request.query_params[param.substr(0, eq)] = param.substr(eq + 1);
            }
        }
    }
    return request;
}

HttpResponse ApiServer::route_request(const HttpRequest& request) {
    StringType path = request.path;
    if (path.length() > 1 && path.back() == '/') {
        path.pop_back();
    }
    if (path == "/health")     return handle_health(request);
    if (path == "/status")     return handle_status(request);
    if (path == "/metrics")    return handle_metrics(request);
    if (path == "/topology")   return handle_topology(request);
    if (path == "/partitions") return handle_partitions(request);
    if (path == "/sessions")   return handle_sessions(request);
    if (path == "/reload")     return handle_reload(request);
    if (path == "/symbols")    return handle_symbols(request);
    if (path.rfind("/lvc/", 0) == 0) return handle_lvc(request);
    return error_response(404, "Not Found: " + request.path);
}

HttpResponse ApiServer::handle_health(const HttpRequest&) {
    HttpResponse r;
    r.body = "{\"status\":\"ok\"}";
    return r;
}

HttpResponse ApiServer::handle_status(const HttpRequest&) {
    if (!agent_) {
        return error_response(500, "agent not available");
    }
    HttpResponse r;
    r.body = serialize_status(agent_->get_status());
    return r;
}

HttpResponse ApiServer::handle_metrics(const HttpRequest& request) {
    if (!agent_) {
        return error_response(500, "agent not available");
    }
    HttpResponse r;
    auto it = request.query_params.find("format");
    if (it != request.query_params.end() && it->second == "json") {
        r.body = agent_->get_metrics_manager().get_metrics_json();
    } else {
        // Prometheus text exposition 0.0.4 by default (and for the old ?format=text).
        r.headers["Content-Type"] = "text/plain; version=0.0.4; charset=utf-8";
        r.body = agent_->get_metrics_manager().get_summary();
    }
    return r;
}

HttpResponse ApiServer::handle_topology(const HttpRequest&) {
    if (!agent_) {
        return error_response(500, "agent not available");
    }
    HttpResponse r;
    r.body = agent_->render_topology();
    return r;
}

HttpResponse ApiServer::handle_partitions(const HttpRequest&) {
    if (!agent_) {
        return error_response(500, "agent not available");
    }
    HttpResponse r;
    r.body = serialize_partitions(agent_->get_status().partitions);
    return r;
}

HttpResponse ApiServer::handle_sessions(const HttpRequest&) {
    if (!agent_) {
        return error_response(500, "agent not available");
    }
    HttpResponse r;
    r.body = serialize_sessions(agent_->get_status().sessions);
    return r;
}

HttpResponse ApiServer::handle_reload(const HttpRequest& request) {
    if (request.method != "POST") {
        return error_response(405, "Method not allowed");
    }
    if (!agent_) {
        return error_response(500, "agent not available");
    }
    std::string err;
    if (!agent_->reload_sessions(err)) {
        return error_response(500, err.empty() ? "reload failed" : err);
    }
    HttpResponse r;
    r.body = "{\"status\":\"reloaded\"}";
    return r;
}

HttpResponse ApiServer::handle_symbols(const HttpRequest& request) {
    if (!agent_) return error_response(500, "agent not available");
    uint32_t from = 1, max = 1000;
    if (auto it = request.query_params.find("from"); it != request.query_params.end()) from = static_cast<uint32_t>(strtoul(it->second.c_str(), nullptr, 10));
    if (auto it = request.query_params.find("max"); it != request.query_params.end()) max = static_cast<uint32_t>(strtoul(it->second.c_str(), nullptr, 10));
    if (max > 100000) max = 100000;
    std::ostringstream j;
    j << "[";
    bool first = true;
    for (const auto& [id, name] : agent_->symbols(from, max)) {
        j << (first ? "" : ",") << "{\"id\":" << id << ",\"name\":\"" << json_escape(name) << "\"}";
        first = false;
    }
    j << "]";
    HttpResponse r;
    r.body = j.str();
    return r;
}

HttpResponse ApiServer::handle_lvc(const HttpRequest& request) {
    if (!agent_) return error_response(500, "agent not available");
    StringType symbol = request.path.substr(5);
    if (symbol.empty()) return error_response(400, "missing symbol");
    StringType type = "TRADE";
    if (auto it = request.query_params.find("type"); it != request.query_params.end()) type = it->second;
    TickHeader h;
    std::vector<uint8_t> payload;
    std::string err;
    if (!agent_->lvc_read(symbol, type, h, payload, err)) return error_response(404, err);
    std::ostringstream j;
    j << "{\"symbol\":\"" << json_escape(symbol) << "\",\"symbol_id\":" << h.symbol_id
      << ",\"msg_type\":" << static_cast<int>(h.msg_type) << ",\"sequence\":" << h.sequence
      << ",\"exchange_ts\":" << h.exchange_ts << ",\"ingest_ts\":" << h.ingest_ts
      << ",\"template_id\":" << h.template_id << ",\"partition_id\":" << h.partition_id
      << ",\"venue_id\":" << h.venue_id << ",\"truncated\":" << ((h.flags & FLAG_LVC_TRUNCATED) ? "true" : "false")
      << ",\"payload_len\":" << h.payload_len << ",\"payload_hex\":\"";
    static const char* hex = "0123456789abcdef";
    for (uint8_t b : payload) { j << hex[b >> 4] << hex[b & 0xF]; }
    j << "\"}";
    HttpResponse r;
    r.body = j.str();
    return r;
}

void ApiServer::send_response(int client_socket, const HttpResponse& response) {
    std::ostringstream out;
    out << "HTTP/1.1 " << response.status_code << " ";
    switch (response.status_code) {
        case 200: out << "OK"; break;
        case 400: out << "Bad Request"; break;
        case 404: out << "Not Found"; break;
        case 405: out << "Method Not Allowed"; break;
        case 500: out << "Internal Server Error"; break;
        default:  out << "Status"; break;
    }
    out << "\r\n";
    for (const auto& [k, v] : response.headers) {
        out << k << ": " << v << "\r\n";
    }
    out << "Content-Length: " << response.body.size() << "\r\n";
    out << "Connection: close\r\n\r\n";
    out << response.body;
    StringType s = out.str();
    size_t sent = 0;
    while (sent < s.size()) {
        ssize_t n = send(client_socket, s.data() + sent, s.size() - sent, MSG_NOSIGNAL);
        if (n <= 0) {
            break;
        }
        sent += static_cast<size_t>(n);
    }
}

HttpResponse ApiServer::error_response(int code, const StringType& message) {
    HttpResponse r;
    r.status_code = code;
    r.body = "{\"error\":\"" + json_escape(message) + "\"}";
    return r;
}

StringType ApiServer::json_escape(const StringType& s) {
    StringType out;
    out.reserve(s.size() + 8);
    for (char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
    return out;
}

StringType ApiServer::serialize_partitions(const std::vector<PartitionStatus>& parts) {
    std::ostringstream j;
    j << "[";
    for (size_t i = 0; i < parts.size(); ++i) {
        const auto& p = parts[i];
        if (i) j << ",";
        j << "{\"name\":\"" << json_escape(p.name) << "\",\"id\":" << p.id
          << ",\"state\":\"" << p.state << "\",\"last_sequence\":" << p.last_sequence
          << ",\"packets\":" << p.packets << ",\"messages\":" << p.messages
          << ",\"unknown_templates\":" << p.unknown_templates << ",\"gaps\":" << p.gaps
          << ",\"last_rx_ns\":" << p.last_rx_ns << ",\"transport\":\"" << p.transport
          << "\",\"transport_down\":" << p.transport_down << ",\"transport_up\":" << p.transport_up;
        if (!p.recap_json.empty()) j << ",\"recap\":" << p.recap_json;
        j << "}";
    }
    j << "]";
    return j.str();
}

StringType ApiServer::serialize_sessions(const std::vector<SessionStatus>& sessions) {
    std::ostringstream j;
    j << "[";
    for (size_t i = 0; i < sessions.size(); ++i) {
        const auto& s = sessions[i];
        if (i) j << ",";
        j << "{\"name\":\"" << json_escape(s.name) << "\",\"id\":" << s.id
          << ",\"owner\":\"" << s.owner << "\",\"ring_path\":\"" << json_escape(s.ring_path)
          << "\",\"epoch\":" << s.epoch << ",\"published\":" << s.published
          << ",\"filtered\":" << s.filtered << ",\"filter_generation\":" << s.filter_generation << "}";
    }
    j << "]";
    return j.str();
}

StringType ApiServer::serialize_status(const AgentStatus& status) {
    std::ostringstream j;
    j << "{\"name\":\"" << json_escape(status.name) << "\",\"version\":\"" << json_escape(status.version)
      << "\",\"running\":" << (status.running ? "true" : "false")
      << ",\"uptime_seconds\":" << status.uptime_seconds << ",\"epoch\":" << status.epoch
      << ",\"partitions\":" << serialize_partitions(status.partitions)
      << ",\"sessions\":" << serialize_sessions(status.sessions) << "}";
    return j.str();
}

} // namespace subplane
