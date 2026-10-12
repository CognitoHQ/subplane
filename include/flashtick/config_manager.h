#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <mutex>
#include <yaml-cpp/yaml.h>

namespace flashtick {

class ConfigManager {
public:
    ConfigManager();
    ~ConfigManager();

    // Load configuration from YAML file
    bool load_config(const std::string& config_path);
    
    // Get configuration value
    template<typename T>
    T get_value(const std::string& key, const T& default_value = T{}) const;
    
    // Get configuration section
    YAML::Node get_section(const std::string& section) const;
    
    // Check if key exists
    bool has_key(const std::string& key) const;
    
    // Get all keys in a section
    std::vector<std::string> get_keys(const std::string& section = "") const;
    
    // Reload configuration
    bool reload_config();
    
    // Get config file path
    const std::string& get_config_path() const { return config_path_; }

private:
    std::string config_path_;
    YAML::Node config_;
    mutable std::mutex mutex_;
    
    YAML::Node get_node(const std::string& key) const;
};

// Template implementations
template<typename T>
T ConfigManager::get_value(const std::string& key, const T& default_value) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto node = get_node(key);
    // INTENT: a missing key yields a Null node, which IsDefined() and would
    // convert to the string "null". Only a present, non-null scalar counts.
    if (node.IsDefined() && !node.IsNull()) {
        return node.as<T>();
    }
    return default_value;
}

} // namespace flashtick
