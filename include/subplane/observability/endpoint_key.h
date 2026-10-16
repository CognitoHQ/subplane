#pragma once

// Canonical endpoint keys for /topology (docs/context/observability.md,
// "Endpoint keys"). One service's downstream key equals another's upstream
// key when they are the two ends of the same stream, so a collector can join
// topology documents without either service knowing the other. The rules are
// normative and pinned by docs/topology-keys.json, which subplane-feeds checks
// too: change one implementation and the vectors, never just one side.
//
// Cold path, pure functions, no I/O: the caller passes the local host name
// and the resolved Aeron directory.

#include <cstdint>
#include <string>
#include <string_view>

namespace subplane::observability {

// "", "*", 0.0.0.0, ::, localhost, 127.0.0.1 and ::1 name this box: they become
// `host`. Any other address is lower-cased and kept as written (no DNS).
std::string host_part(std::string_view address, std::string_view host);

// aeron:ipc -> aeron-ipc://<host><aeron_dir>#<stream> (IPC is per media driver).
// aeron:udp -> aeron-udp://<endpoint>#<stream>, from `endpoint=`, else
// `control=`; other parameters (term-length, control-mode,
// response-correlation-id, ...) are dropped. Empty if the channel is neither.
std::string aeron_key(std::string_view channel, int32_t stream_id, std::string_view aeron_dir, std::string_view host);

// UDP: udp-mcast://<group>:<port> for a group, else udp://<address>:<port>.
std::string udp_key(std::string_view group, std::string_view address, uint16_t port, std::string_view host);

// tcp://<address>:<port>
std::string tcp_key(std::string_view address, uint16_t port, std::string_view host);

// shm-ring://<host><path>
std::string shm_ring_key(std::string_view path, std::string_view host);

// A URL (wss://, https://, ...): scheme and host lower-cased, the scheme's
// default port, user info, query and fragment dropped; the path is kept.
std::string url_key(std::string_view url);

} // namespace subplane::observability
