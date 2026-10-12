#include "flashtick/registry/component_registry.h"

#include "flashtick/transport/udp_multicast.h"

namespace flashtick {

namespace {
constexpr ComponentEntry<Transport> kTransports[] = {
    {"udp_multicast", &UdpMulticastTransport::create},
    {"udp",           &UdpMulticastTransport::create}, // alias: unicast when group is empty
};
} // namespace

std::span<const ComponentEntry<Transport>> transport_components() {
    return kTransports;
}

} // namespace flashtick
