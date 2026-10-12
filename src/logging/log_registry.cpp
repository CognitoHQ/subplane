#include "flashtick/logging/log_registry.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <yaml-cpp/yaml.h>

namespace flashtick {

bool LogRegistry::initialize(const std::string& config) {
    try {
        if (!parse_config(config)) {
            std::cerr << "Failed to parse log registry configuration" << std::endl;
            return false;
        }
        
        min_log_level_.store(config_.min_level);
        
        // Open log file if specified
        if (!config_.log_file.empty()) {
            log_file_.open(config_.log_file, std::ios::app);
            if (!log_file_.is_open()) {
                std::cerr << "Failed to open log file: " << config_.log_file << std::endl;
                return false;
            }
        }
        
        std::cout << "Log registry initialized" << std::endl;
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception during log registry initialization: " << e.what() << std::endl;
        return false;
    }
}

void LogRegistry::log(LogLevel level, const std::string& component, const std::string& event, 
                     const std::string& message, const std::unordered_map<std::string, std::string>& metadata) {
    if (!config_.enabled || level < min_log_level_.load()) {
        return;
    }
    
    LogEntry entry(level, component, event, message, metadata);
    
    // Store entry
    {
        std::lock_guard<std::mutex> lock(entries_mutex_);
        log_entries_.push_back(entry);
        trim_entries();
    }
    
    // Output to console
    if (config_.console_output) {
        write_to_console(entry);
    }
    
    // Write to file
    if (log_file_.is_open()) {
        write_to_file(entry);
    }
    
    // Call output callback
    call_output_callback(entry);
}

std::vector<LogEntry> LogRegistry::get_recent_entries(size_t count) const {
    std::lock_guard<std::mutex> lock(entries_mutex_);
    
    size_t start = log_entries_.size() > count ? log_entries_.size() - count : 0;
    return std::vector<LogEntry>(log_entries_.begin() + static_cast<std::ptrdiff_t>(start), log_entries_.end());
}

std::vector<LogEntry> LogRegistry::get_entries_by_component(const std::string& component, size_t count) const {
    std::lock_guard<std::mutex> lock(entries_mutex_);
    
    std::vector<LogEntry> result;
    result.reserve(count);
    
    // Search from the end (most recent first)
    for (auto it = log_entries_.rbegin(); it != log_entries_.rend() && result.size() < count; ++it) {
        if (it->component == component) {
            result.push_back(*it);
        }
    }
    
    // Reverse to get chronological order
    std::reverse(result.begin(), result.end());
    return result;
}

std::vector<LogEntry> LogRegistry::get_entries_by_event(const std::string& event, size_t count) const {
    std::lock_guard<std::mutex> lock(entries_mutex_);
    
    std::vector<LogEntry> result;
    result.reserve(count);
    
    // Search from the end (most recent first)
    for (auto it = log_entries_.rbegin(); it != log_entries_.rend() && result.size() < count; ++it) {
        if (it->event == event) {
            result.push_back(*it);
        }
    }
    
    // Reverse to get chronological order
    std::reverse(result.begin(), result.end());
    return result;
}

std::vector<LogEntry> LogRegistry::get_entries_by_level(LogLevel level, size_t count) const {
    std::lock_guard<std::mutex> lock(entries_mutex_);
    
    std::vector<LogEntry> result;
    result.reserve(count);
    
    // Search from the end (most recent first)
    for (auto it = log_entries_.rbegin(); it != log_entries_.rend() && result.size() < count; ++it) {
        if (it->level == level) {
            result.push_back(*it);
        }
    }
    
    // Reverse to get chronological order
    std::reverse(result.begin(), result.end());
    return result;
}

void LogRegistry::set_output_callback(std::function<void(const LogEntry&)> callback) {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    output_callback_ = callback;
}

void LogRegistry::clear() {
    std::lock_guard<std::mutex> lock(entries_mutex_);
    log_entries_.clear();
}

void LogRegistry::shutdown() {
    if (log_file_.is_open()) {
        log_file_.close();
    }
}

bool LogRegistry::parse_config(const std::string& config_str) {
    try {
        YAML::Node config = YAML::Load(config_str);
        
        config_.enabled = config["enabled"].as<bool>(true);
        config_.min_level = string_to_log_level(config["min_level"].as<std::string>("INFO"));
        config_.max_entries = config["max_entries"].as<size_t>(10000);
        config_.console_output = config["console_output"].as<bool>(true);
        config_.log_file = config["log_file"].as<std::string>("");
        config_.output_format = config["output_format"].as<std::string>("text");
        config_.include_metadata = config["include_metadata"].as<bool>(true);
        
        return true;
        
    } catch (const YAML::Exception& e) {
        std::cerr << "YAML parsing error: " << e.what() << std::endl;
        return false;
    }
}

void LogRegistry::write_to_console(const LogEntry& entry) const {
    if (config_.output_format == "json") {
        std::cout << entry.to_json() << std::endl;
    } else {
        std::cout << entry.to_string() << std::endl;
    }
}

void LogRegistry::write_to_file(const LogEntry& entry) {
    std::lock_guard<std::mutex> lock(file_mutex_);
    
    if (config_.output_format == "json") {
        log_file_ << entry.to_json() << std::endl;
    } else {
        log_file_ << entry.to_string() << std::endl;
    }
    
    log_file_.flush();
}

void LogRegistry::call_output_callback(const LogEntry& entry) {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    if (output_callback_) {
        output_callback_(entry);
    }
}

void LogRegistry::trim_entries() {
    if (log_entries_.size() > config_.max_entries) {
        size_t excess = log_entries_.size() - config_.max_entries;
        log_entries_.erase(log_entries_.begin(), log_entries_.begin() + static_cast<std::ptrdiff_t>(excess));
    }
}

} // namespace flashtick
