#pragma once

// Static component registry. Components are compiled into the binary and
// selected by the `type:` string in YAML. No self-registering statics: the
// tables live in src/registry/builtin_components.cpp and are greppable.

#include <memory>
#include <span>
#include <string>
#include <string_view>

#include <yaml-cpp/yaml.h>

namespace subplane {

class Transport;
class RecapTransport;

template <class Iface>
struct ComponentEntry {
    const char* type;
    std::unique_ptr<Iface> (*make)(const YAML::Node& cfg, std::string& err);
};

std::span<const ComponentEntry<Transport>> transport_components();
// Recap components, selected by a partition's `recap.type`.
std::span<const ComponentEntry<RecapTransport>> recap_components();

// Decoders are not constructed through a factory (they are held by value in
// Partition to avoid per-message virtual dispatch); this is the list of valid
// `decoder.type` values for validation and error messages.
std::span<const char* const> decoder_types();

// Finds `type` in the table and constructs it. On an unknown type, err lists
// the available types so a config typo is a one-line fix.
template <class Iface>
std::unique_ptr<Iface> make_component(std::span<const ComponentEntry<Iface>> table,
                                      std::string_view type, const YAML::Node& cfg, std::string& err) {
    for (const auto& e : table) {
        if (type == e.type) {
            return e.make(cfg, err);
        }
    }
    err = "unknown component type '" + std::string(type) + "'; available:";
    for (const auto& e : table) {
        err += ' ';
        err += e.type;
    }
    return nullptr;
}

} // namespace subplane
