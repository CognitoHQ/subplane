#include "subplane/api_server.h"
#include "subplane/pipeline/topic_registry.h"

#include "subplane/agent.h"
#include "subplane/logging/log_registry.h"
#include "subplane/metrics/metrics_manager.h"

#include <algorithm>
#include <arpa/inet.h>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <strings.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <algorithm>
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
    // INTENT: TCP may deliver a request in several segments, so read until the
    // end of the headers plus any Content-Length body, not one read(). With a
    // single read the server parsed a truncated request, answered, and closed
    // with unread bytes queued, which makes the kernel reset the connection:
    // the client then lost the reply or failed to send the rest (seen in CI
    // on POSTs). The timeout bounds a client that never finishes its request.
    constexpr size_t kMaxRequest = 16384;
    timeval tv{};
    tv.tv_sec = 2;
    ::setsockopt(client_socket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    std::string raw;
    char buffer[4096];
    size_t need = 0; // total bytes once the headers are complete
    while (raw.size() < kMaxRequest) {
        const ssize_t n = ::read(client_socket, buffer, sizeof buffer);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) break; // closed, timed out or failed: answer what arrived, if anything
        raw.append(buffer, static_cast<size_t>(n));
        if (need == 0) {
            const size_t end = raw.find("\r\n\r\n");
            if (end == std::string::npos) continue;
            need = end + 4;
            for (size_t p = raw.find('\n'); p != std::string::npos && p < end; p = raw.find('\n', p + 1)) {
                if (raw.size() >= p + 16 && strncasecmp(raw.c_str() + p + 1, "content-length:", 15) == 0) {
                    need += std::strtoul(raw.c_str() + p + 16, nullptr, 10);
                    break;
                }
            }
        }
        if (raw.size() >= need) break;
    }
    if (raw.empty()) return;
    if (raw.size() >= kMaxRequest && (need == 0 || raw.size() < need)) {
        send_response(client_socket, error_response(413, "request too large"));
        return;
    }
    HttpRequest request = parse_request(raw);
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
    if (path == "/topics")     return handle_topics(request);
    if (path == "/partitions") return handle_partitions(request);
    if (path == "/sessions")   return handle_sessions(request);
    if (path == "/reload")     return handle_reload(request);
    if (path == "/symbols")    return handle_symbols(request);
    if (path.rfind("/lvc/", 0) == 0) return handle_lvc(request);
    if (path.rfind("/sessions/", 0) == 0 && path.size() > 16 && path.compare(path.size() - 6, 6, "/trace") == 0) return handle_trace(request, path);
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

HttpResponse ApiServer::handle_topics(const HttpRequest&) {
    if (!agent_) {
        return error_response(500, "agent not available");
    }
    HttpResponse r;
    r.body = serialize_topics(agent_->get_status());
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

// POST /sessions/<name>/trace?on=1|0[&pid=N][&sample=N]: latency tracing for a
// session's readers, all of them or one process's.
HttpResponse ApiServer::handle_trace(const HttpRequest& request, const StringType& path) {
    if (request.method != "POST") return error_response(405, "Method not allowed");
    if (!agent_) return error_response(500, "agent not available");
    const std::string session = path.substr(10, path.size() - 16);
    auto q = [&](const char* k) { auto it = request.query_params.find(k); return it == request.query_params.end() ? std::string() : it->second; };
    const std::string on = q("on");
    if (on != "0" && on != "1") return error_response(400, "want on=1 or on=0");
    const unsigned long pid = std::strtoul(q("pid").c_str(), nullptr, 10);
    const unsigned long sample = std::strtoul(q("sample").c_str(), nullptr, 10);
    if (pid > 0x7FFFFFFFul || sample > 1000000ul) return error_response(400, "pid or sample out of range");
    uint32_t matched = 0;
    std::string err;
    if (!agent_->set_reader_trace(session, on == "1", static_cast<int32_t>(pid), static_cast<uint32_t>(sample), matched, err)) {
        return error_response(404, err);
    }
    HttpResponse r;
    r.body = "{\"session\":\"" + json_escape(session) + "\",\"tracing\":" + (on == "1" ? "true" : "false") +
             ",\"pid\":" + std::to_string(pid) + ",\"slots\":" + std::to_string(matched) + "}";
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
    std::vector<std::string> ignored;
    if (!agent_->reload_sessions(err, &ignored)) {
        return error_response(500, err.empty() ? "reload failed" : err);
    }
    HttpResponse r;
    std::ostringstream j;
    j << "{\"status\":\"reloaded\",\"ignored\":[";
    for (size_t i = 0; i < ignored.size(); ++i) j << (i ? "," : "") << "\"" << json_escape(ignored[i]) << "\"";
    j << "]}";
    r.body = j.str();
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
      << ",\"template_id\":" << h.template_id << ",\"topic_id\":" << h.topic_id
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
        j << "{\"name\":\"" << json_escape(p.name) << "\",\"topic\":\"" << json_escape(p.topic)
          << "\",\"topic_id\":" << p.topic_id << ",\"partition_num\":" << p.partition_num
          << ",\"stream_id\":" << p.stream_id << ",\"state\":\"" << p.state << "\",\"last_sequence\":" << p.last_sequence
          << ",\"packets\":" << p.packets << ",\"messages\":" << p.messages
          << ",\"unknown_templates\":" << p.unknown_templates << ",\"gaps\":" << p.gaps
          << ",\"last_rx_ns\":" << p.last_rx_ns << ",\"transport\":\"" << p.transport
          << "\",\"transport_down\":" << p.transport_down << ",\"transport_up\":" << p.transport_up
          << ",\"stream_mismatch\":" << p.stream_mismatch << ",\"last_stream_stamp\":" << p.last_stream_stamp
          << ",\"inst_gaps\":" << p.inst_gaps << ",\"inst_dups\":" << p.inst_dups;
        if (!p.recap_json.empty()) j << ",\"recap\":" << p.recap_json;
        if (!p.tap_ring.empty()) {
            j << ",\"tap\":{\"ring\":\"" << json_escape(p.tap_ring) << "\",\"records\":" << p.tap_records
              << ",\"dropped\":" << p.tap_dropped << ",\"bytes\":" << p.tap_bytes << ",\"readers\":" << p.tap_readers
              << ",\"lag_bytes\":" << p.tap_lag_bytes << "}";
        }
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
          << ",\"filtered\":" << s.filtered << ",\"filter_generation\":" << s.filter_generation
          << ",\"reader_flags\":" << s.reader_flags << ",\"topics\":[";
        for (size_t k = 0; k < s.topics.size(); ++k) j << (k ? "," : "") << "\"" << json_escape(s.topics[k]) << "\"";
        j << "],\"readers\":[";
        for (size_t k = 0; k < s.readers.size(); ++k) {
            const auto& r = s.readers[k];
            j << (k ? "," : "") << "{\"slot\":" << r.slot << ",\"pid\":" << r.pid << ",\"name\":\"" << json_escape(r.name)
              << "\",\"lag_bytes\":" << (s.ring_head >= r.read_pos ? s.ring_head - r.read_pos : 0) << ",\"records\":" << r.records
              << ",\"overruns\":" << r.overruns << ",\"heartbeat_ns\":" << r.heartbeat_ns
              << ",\"tracing\":" << ((r.flags_active & 1u) ? "true" : "false") << "}";
        }
        j << "]}";
    }
    j << "]";
    return j.str();
}

// A topic's view: its partitions as this agent reads them, and the sessions
// whose patterns take it.
StringType ApiServer::serialize_topics(const AgentStatus& status) {
    std::ostringstream j;
    j << "[";
    std::vector<std::string> done;
    for (const auto& p : status.partitions) {
        if (std::find(done.begin(), done.end(), p.topic) != done.end()) continue;
        done.push_back(p.topic);
        uint32_t degraded = 0;
        std::ostringstream parts;
        bool first = true;
        for (const auto& q : status.partitions) {
            if (q.topic != p.topic) continue;
            if (q.state == "degraded") ++degraded;
            parts << (first ? "" : ",") << "{\"partition_num\":" << q.partition_num << ",\"stream_id\":" << q.stream_id
                  << ",\"state\":\"" << q.state << "\"}";
            first = false;
        }
        j << (done.size() > 1 ? "," : "") << "{\"name\":\"" << json_escape(p.topic) << "\",\"topic_id\":" << p.topic_id
          << ",\"degraded\":" << degraded << ",\"partitions\":[" << parts.str() << "],\"sessions\":[";
        first = true;
        for (const auto& s : status.sessions) {
            bool match = s.topics.empty();
            for (const auto& pat : s.topics) if (topic_matches(pat, p.topic)) { match = true; break; }
            if (!match) continue;
            j << (first ? "" : ",") << "\"" << json_escape(s.name) << "\"";
            first = false;
        }
        j << "]}";
    }
    j << "]";
    return j.str();
}

StringType ApiServer::serialize_status(const AgentStatus& status) {
    std::ostringstream j;
    j << "{\"name\":\"" << json_escape(status.name) << "\",\"version\":\"" << json_escape(status.version)
      << "\",\"running\":" << (status.running ? "true" : "false")
      << ",\"uptime_seconds\":" << status.uptime_seconds << ",\"epoch\":" << status.epoch
      << ",\"topics\":" << serialize_topics(status)
      << ",\"partitions\":" << serialize_partitions(status.partitions)
      << ",\"sessions\":" << serialize_sessions(status.sessions) << "}";
    return j.str();
}

} // namespace subplane
