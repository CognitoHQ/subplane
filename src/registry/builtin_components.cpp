#include "flashtick/registry/component_registry.h"

#include "flashtick/transport/tcp_stream.h"
#include "flashtick/transport/udp_multicast.h"

namespace flashtick {

namespace {
constexpr ComponentEntry<Transport> kTransports[] = {
    {"udp_multicast", &UdpMulticastTransport::create},
    {"udp",           &UdpMulticastTransport::create}, // alias: unicast when group is empty
    {"tcp_stream",    &TcpStreamTransport::create},
};
constexpr const char* kDecoderTypes[] = {"sbe_routing", "dbn", "flatbuffers"};
} // namespace

std::span<const ComponentEntry<Transport>> transport_components() {
    return kTransports;
}

std::span<const char* const> decoder_types() {
    return kDecoderTypes;
}

} // namespace flashtick
