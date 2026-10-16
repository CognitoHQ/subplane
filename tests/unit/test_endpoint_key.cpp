#include <doctest.h>

#include "subplane/observability/endpoint_key.h"

#include <yaml-cpp/yaml.h>

#include <string>

using namespace subplane::observability;

namespace {
std::string vectors_path() {
    std::string f = __FILE__;
    return f.substr(0, f.find_last_of('/')) + "/../../docs/topology-keys.json";
}
} // namespace

TEST_CASE("endpoint keys: every vector in docs/topology-keys.json") {
    const YAML::Node v = YAML::LoadFile(vectors_path()); // JSON is YAML
    REQUIRE(v["schema"].as<std::string>() == "subplane.topology-keys/1");
    int n = 0;
    for (const auto& t : v["aeron"]) {
        INFO(t["channel"].as<std::string>());
        CHECK(aeron_key(t["channel"].as<std::string>(), t["stream"].as<int32_t>(), t["aeron_dir"].as<std::string>(),
                        t["host"].as<std::string>()) == t["key"].as<std::string>());
        ++n;
    }
    for (const auto& t : v["udp"]) {
        CHECK(udp_key(t["group"].as<std::string>(), t["address"].as<std::string>(), t["port"].as<uint16_t>(),
                      t["host"].as<std::string>()) == t["key"].as<std::string>());
        ++n;
    }
    for (const auto& t : v["tcp"]) {
        CHECK(tcp_key(t["address"].as<std::string>(), t["port"].as<uint16_t>(), t["host"].as<std::string>()) ==
              t["key"].as<std::string>());
        ++n;
    }
    for (const auto& t : v["shm_ring"]) {
        CHECK(shm_ring_key(t["path"].as<std::string>(), t["host"].as<std::string>()) == t["key"].as<std::string>());
        ++n;
    }
    for (const auto& t : v["url"]) {
        INFO(t["url"].as<std::string>());
        CHECK(url_key(t["url"].as<std::string>()) == t["key"].as<std::string>());
        ++n;
    }
    CHECK(n == 20);
}
