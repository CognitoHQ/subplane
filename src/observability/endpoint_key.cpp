#include "subplane/observability/endpoint_key.h"

#include <cctype>

namespace subplane::observability {

namespace {

std::string lower(std::string_view s) {
    std::string o(s);
    for (char& c : o) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return o;
}

// Value of `key=` in an Aeron URI's parameters ("a=1|b=2"), empty if absent.
std::string_view param(std::string_view params, std::string_view key) {
    // INTENT: index arithmetic rather than substr(0, npos) + find: GCC 11
    // reports a false -Wstringop-overread on that pattern under -Werror.
    size_t pos = 0;
    while (pos < params.size()) {
        size_t bar = pos;
        while (bar < params.size() && params[bar] != '|') ++bar;
        size_t eq = pos;
        while (eq < bar && params[eq] != '=') ++eq;
        if (eq < bar && params.substr(pos, eq - pos) == key) return params.substr(eq + 1, bar - eq - 1);
        pos = bar + 1;
    }
    return {};
}

// "host:port" or "[v6]:port" with the host part normalized.
std::string host_port(std::string_view hp, std::string_view host) {
    size_t colon = std::string_view::npos;
    if (!hp.empty() && hp.front() == '[') {
        const size_t close = hp.find(']');
        if (close != std::string_view::npos && close + 1 < hp.size() && hp[close + 1] == ':') colon = close + 1;
        const std::string_view addr = close == std::string_view::npos ? hp : hp.substr(1, close - 1);
        return host_part(addr, host) + (colon == std::string_view::npos ? "" : std::string(hp.substr(colon)));
    }
    colon = hp.rfind(':');
    if (colon == std::string_view::npos) return host_part(hp, host);
    return host_part(hp.substr(0, colon), host) + std::string(hp.substr(colon));
}

std::string trim_slashes(std::string_view p) {
    while (p.size() > 1 && p.back() == '/') p.remove_suffix(1);
    return std::string(p);
}

} // namespace

std::string host_part(std::string_view address, std::string_view host) {
    const std::string a = lower(address);
    if (a.empty() || a == "*" || a == "0.0.0.0" || a == "::" || a == "localhost" || a == "127.0.0.1" || a == "::1") {
        return lower(host);
    }
    return a;
}

std::string aeron_key(std::string_view channel, int32_t stream_id, std::string_view aeron_dir, std::string_view host) {
    std::string_view c = channel;
    if (c.rfind("aeron-spy:", 0) == 0) c.remove_prefix(10); // a spy reads the same stream
    if (c.rfind("aeron:", 0) != 0) return {};
    c.remove_prefix(6);
    const size_t q = c.find('?');
    const std::string_view media = c.substr(0, q);
    const std::string_view params = q == std::string_view::npos ? std::string_view{} : c.substr(q + 1);
    const std::string stream = "#" + std::to_string(stream_id);
    if (media == "ipc") return "aeron-ipc://" + lower(host) + trim_slashes(aeron_dir) + stream;
    if (media != "udp") return {};
    std::string_view ep = param(params, "endpoint");
    if (ep.empty()) ep = param(params, "control");
    if (ep.empty()) return {};
    return "aeron-udp://" + host_port(ep, host) + stream;
}

std::string udp_key(std::string_view group, std::string_view address, uint16_t port, std::string_view host) {
    if (!group.empty()) return "udp-mcast://" + lower(group) + ":" + std::to_string(port);
    return "udp://" + host_part(address, host) + ":" + std::to_string(port);
}

std::string tcp_key(std::string_view address, uint16_t port, std::string_view host) {
    return "tcp://" + host_part(address, host) + ":" + std::to_string(port);
}

std::string shm_ring_key(std::string_view path, std::string_view host) {
    return "shm-ring://" + lower(host) + std::string(path);
}

std::string url_key(std::string_view url) {
    const size_t sep = url.find("://");
    if (sep == std::string_view::npos) return std::string(url);
    const std::string scheme = lower(url.substr(0, sep));
    std::string_view rest = url.substr(sep + 3);
    rest = rest.substr(0, rest.find_first_of("?#"));
    const size_t slash = rest.find('/');
    std::string_view authority = rest.substr(0, slash);
    const std::string_view path = slash == std::string_view::npos ? std::string_view{} : rest.substr(slash);
    if (const size_t at = authority.rfind('@'); at != std::string_view::npos) authority.remove_prefix(at + 1);
    std::string auth = lower(authority);
    const size_t colon = auth.rfind(':');
    if (colon != std::string::npos && auth.find(']', colon) == std::string::npos) {
        const std::string port = auth.substr(colon + 1);
        const bool dflt = ((scheme == "https" || scheme == "wss") && port == "443") ||
                          ((scheme == "http" || scheme == "ws") && port == "80");
        if (dflt) auth.resize(colon);
    }
    return scheme + "://" + auth + std::string(path);
}

} // namespace subplane::observability
