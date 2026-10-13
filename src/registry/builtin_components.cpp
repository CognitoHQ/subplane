#include "subplane/registry/component_registry.h"

#include "subplane/transport/tcp_stream.h"
#include "subplane/transport/udp_multicast.h"
#if SUBPLANE_WITH_AERON
#include "subplane/transport/aeron_transport.h"
#endif

namespace subplane {

namespace {
constexpr ComponentEntry<Transport> kTransports[] = {
    {"udp_multicast", &UdpMulticastTransport::create},
    {"udp",           &UdpMulticastTransport::create}, // alias: unicast when group is empty
    {"tcp_stream",    &TcpStreamTransport::create},
#if SUBPLANE_WITH_AERON
    {"aeron",         &AeronTransport::create},
#endif
};
constexpr const char* kDecoderTypes[] = {"sbe_routing", "dbn", "flatbuffers"};
} // namespace

std::span<const ComponentEntry<Transport>> transport_components() {
    return kTransports;
}

std::span<const char* const> decoder_types() {
    return kDecoderTypes;
}

} // namespace subplane
