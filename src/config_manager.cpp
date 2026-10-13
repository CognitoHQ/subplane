#include "subplane/config_manager.h"
#include <iostream>
#include <fstream>

namespace subplane {

ConfigManager::ConfigManager() = default;

ConfigManager::~ConfigManager() = default;

bool ConfigManager::load_config(const std::string& config_path) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    try {
        config_path_ = config_path;
        config_ = YAML::LoadFile(config_path);
        
        if (!config_.IsMap()) {
            std::cerr << "Configuration file is not a valid YAML map: " << config_path << std::endl;
            return false;
        }
        
        std::cout << "Configuration loaded from " << config_path << std::endl;
        return true;
        
    } catch (const YAML::Exception& e) {
        std::cerr << "YAML parsing error: " << e.what() << std::endl;
        return false;
    } catch (const std::exception& e) {
        std::cerr << "Error loading configuration: " << e.what() << std::endl;
        return false;
    }
}

YAML::Node ConfigManager::get_section(const std::string& section) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return get_node(section);
}

bool ConfigManager::has_key(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return get_node(key).IsDefined();
}

std::vector<std::string> ConfigManager::get_keys(const std::string& section) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> keys;
    
    YAML::Node node = section.empty() ? config_ : get_node(section);
    
    if (node.IsMap()) {
        for (const auto& pair : node) {
            keys.push_back(pair.first.as<std::string>());
        }
    }
    
    return keys;
}

bool ConfigManager::reload_config() {
    if (config_path_.empty()) {
        std::cerr << "No configuration file path set" << std::endl;
        return false;
    }
    
    return load_config(config_path_);
}

YAML::Node ConfigManager::get_node(const std::string& key) const {
    if (key.empty()) {
        return config_;
    }
    
    // Split key by dots for nested access
    std::vector<std::string> parts;
    std::string current;
    
    for (char c : key) {
        if (c == '.') {
            if (!current.empty()) {
                parts.push_back(current);
                current.clear();
            }
        } else {
            current += c;
        }
    }
    
    if (!current.empty()) {
        parts.push_back(current);
    }
    
    // Navigate through the YAML structure
    // INTENT: yaml-cpp Node assignment aliases; `node = node[part]` makes a
    // node refer to its own child and corrupts the tree ("bad conversion" on
    // later reads). reset() rebinds without aliasing. Do not "simplify" this.
    YAML::Node node = YAML::Clone(config_);
    for (const auto& part : parts) {
        if (!node.IsMap()) {
            return YAML::Node();
        }
        YAML::Node child = node[part];
        if (!child.IsDefined()) {
            return YAML::Node();
        }
        node.reset(child);
    }
    
    return node;
}

} // namespace subplane
