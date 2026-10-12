#include "flashtick/persistence_manager.h"
#include <iostream>
#include <fstream>
#include <filesystem>
#include <algorithm>

namespace flashtick {

PersistenceManager::PersistenceManager() = default;

PersistenceManager::~PersistenceManager() {
    shutdown();
}

bool PersistenceManager::initialize(const PersistenceConfig& config) {
    config_ = config;
    
    // Create disk directory if needed
    if (config_.type == PersistenceConfig::Type::DISK && !config_.path.empty()) {
        try {
            std::filesystem::create_directories(config_.path);
        } catch (const std::exception& e) {
            std::cerr << "Failed to create persistence directory: " << e.what() << std::endl;
            return false;
        }
        disk_path_ = config_.path;
    }
    
    std::cout << "Persistence manager initialized with type: " 
              << (config_.type == PersistenceConfig::Type::MEMORY ? "memory" :
                  config_.type == PersistenceConfig::Type::DISK ? "disk" : "database")
              << std::endl;
    
    return true;
}

bool PersistenceManager::store(const std::string& key, const std::vector<uint8_t>& data) {
    switch (config_.type) {
        case PersistenceConfig::Type::MEMORY:
            return store_memory(key, data);
        case PersistenceConfig::Type::DISK:
            return store_disk(key, data);
        case PersistenceConfig::Type::DATABASE:
            return store_database(key, data);
        default:
            std::cerr << "Unknown persistence type" << std::endl;
            return false;
    }
}

std::optional<std::vector<uint8_t>> PersistenceManager::retrieve(const std::string& key) const {
    switch (config_.type) {
        case PersistenceConfig::Type::MEMORY:
            return retrieve_memory(key);
        case PersistenceConfig::Type::DISK:
            return retrieve_disk(key);
        case PersistenceConfig::Type::DATABASE:
            return retrieve_database(key);
        default:
            std::cerr << "Unknown persistence type" << std::endl;
            return std::nullopt;
    }
}

bool PersistenceManager::exists(const std::string& key) const {
    switch (config_.type) {
        case PersistenceConfig::Type::MEMORY: {
            std::lock_guard<std::mutex> lock(memory_mutex_);
            return memory_storage_.find(key) != memory_storage_.end();
        }
        case PersistenceConfig::Type::DISK: {
            std::lock_guard<std::mutex> lock(disk_mutex_);
            return std::filesystem::exists(get_disk_path(key));
        }
        case PersistenceConfig::Type::DATABASE:
            // Placeholder for database implementation
            return false;
        default:
            return false;
    }
}

bool PersistenceManager::remove(const std::string& key) {
    switch (config_.type) {
        case PersistenceConfig::Type::MEMORY: {
            std::lock_guard<std::mutex> lock(memory_mutex_);
            auto it = memory_storage_.find(key);
            if (it != memory_storage_.end()) {
                memory_usage_ -= it->second.data.size();
                memory_storage_.erase(it);
                return true;
            }
            return false;
        }
        case PersistenceConfig::Type::DISK: {
            std::lock_guard<std::mutex> lock(disk_mutex_);
            std::string path = get_disk_path(key);
            if (std::filesystem::exists(path)) {
                try {
                    auto size = std::filesystem::file_size(path);
                    std::filesystem::remove(path);
                    disk_usage_ -= size;
                    return true;
                } catch (const std::exception& e) {
                    std::cerr << "Failed to remove file: " << e.what() << std::endl;
                }
            }
            return false;
        }
        case PersistenceConfig::Type::DATABASE:
            // Placeholder for database implementation
            return false;
        default:
            return false;
    }
}

std::vector<std::string> PersistenceManager::list_keys() const {
    std::vector<std::string> keys;
    
    switch (config_.type) {
        case PersistenceConfig::Type::MEMORY: {
            std::lock_guard<std::mutex> lock(memory_mutex_);
            for (const auto& [key, entry] : memory_storage_) {
                keys.push_back(key);
            }
            break;
        }
        case PersistenceConfig::Type::DISK: {
            std::lock_guard<std::mutex> lock(disk_mutex_);
            try {
                for (const auto& entry : std::filesystem::directory_iterator(disk_path_)) {
                    if (entry.is_regular_file()) {
                        keys.push_back(entry.path().filename().string());
                    }
                }
            } catch (const std::exception& e) {
                std::cerr << "Failed to list disk keys: " << e.what() << std::endl;
            }
            break;
        }
        case PersistenceConfig::Type::DATABASE:
            // Placeholder for database implementation
            break;
    }
    
    return keys;
}

PersistenceManager::Stats PersistenceManager::get_stats() const {
    Stats stats{};
    
    switch (config_.type) {
        case PersistenceConfig::Type::MEMORY: {
            std::lock_guard<std::mutex> lock(memory_mutex_);
            stats.total_keys = memory_storage_.size();
            stats.total_size = memory_usage_;
            stats.memory_usage = memory_usage_;
            break;
        }
        case PersistenceConfig::Type::DISK: {
            std::lock_guard<std::mutex> lock(disk_mutex_);
            try {
                size_t key_count = 0;
                for (const auto& entry : std::filesystem::directory_iterator(disk_path_)) {
                    if (entry.is_regular_file()) {
                        key_count++;
                    }
                }
                stats.total_keys = key_count;
                stats.total_size = disk_usage_;
                stats.disk_usage = disk_usage_;
            } catch (const std::exception& e) {
                std::cerr << "Failed to get disk stats: " << e.what() << std::endl;
            }
            break;
        }
        case PersistenceConfig::Type::DATABASE:
            // Placeholder for database implementation
            break;
    }
    
    return stats;
}

void PersistenceManager::cleanup() {
    switch (config_.type) {
        case PersistenceConfig::Type::MEMORY:
            cleanup_memory();
            break;
        case PersistenceConfig::Type::DISK:
            cleanup_disk();
            break;
        case PersistenceConfig::Type::DATABASE:
            cleanup_database();
            break;
    }
}

void PersistenceManager::shutdown() {
    cleanup();
}

// Memory storage implementations
bool PersistenceManager::store_memory(const std::string& key, const std::vector<uint8_t>& data) {
    std::lock_guard<std::mutex> lock(memory_mutex_);
    
    // Check if we need to cleanup to make space
    if (memory_usage_ + data.size() > config_.max_memory_size) {
        cleanup_memory();
    }
    
    MemoryEntry entry;
    entry.data = data;
    entry.timestamp = std::chrono::system_clock::now();
    
    // Remove existing entry if it exists
    auto it = memory_storage_.find(key);
    if (it != memory_storage_.end()) {
        memory_usage_ -= it->second.data.size();
    }
    
    memory_storage_[key] = std::move(entry);
    memory_usage_ += data.size();
    
    return true;
}

std::optional<std::vector<uint8_t>> PersistenceManager::retrieve_memory(const std::string& key) const {
    std::lock_guard<std::mutex> lock(memory_mutex_);
    
    auto it = memory_storage_.find(key);
    if (it != memory_storage_.end()) {
        if (is_expired(it->second.timestamp)) {
            return std::nullopt;
        }
        return it->second.data;
    }
    
    return std::nullopt;
}

void PersistenceManager::cleanup_memory() {
    auto it = memory_storage_.begin();
    
    while (it != memory_storage_.end()) {
        if (is_expired(it->second.timestamp)) {
            memory_usage_ -= it->second.data.size();
            it = memory_storage_.erase(it);
        } else {
            ++it;
        }
    }
}

// Disk storage implementations
bool PersistenceManager::store_disk(const std::string& key, const std::vector<uint8_t>& data) {
    std::lock_guard<std::mutex> lock(disk_mutex_);
    
    std::string path = get_disk_path(key);
    
    try {
        // Remove existing file if it exists
        if (std::filesystem::exists(path)) {
            disk_usage_ -= std::filesystem::file_size(path);
            std::filesystem::remove(path);
        }
        
        // Write new file
        std::ofstream file(path, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "Failed to open file for writing: " << path << std::endl;
            return false;
        }
        
        file.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
        file.close();
        
        disk_usage_ += data.size();
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Failed to store to disk: " << e.what() << std::endl;
        return false;
    }
}

std::optional<std::vector<uint8_t>> PersistenceManager::retrieve_disk(const std::string& key) const {
    std::lock_guard<std::mutex> lock(disk_mutex_);
    
    std::string path = get_disk_path(key);
    
    if (!std::filesystem::exists(path)) {
        return std::nullopt;
    }
    
    try {
        auto size = std::filesystem::file_size(path);
        std::vector<uint8_t> data(size);
        
        std::ifstream file(path, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "Failed to open file for reading: " << path << std::endl;
            return std::nullopt;
        }
        
        file.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(size));
        file.close();
        
        return data;
        
    } catch (const std::exception& e) {
        std::cerr << "Failed to retrieve from disk: " << e.what() << std::endl;
        return std::nullopt;
    }
}

void PersistenceManager::cleanup_disk() {
    try {
        for (const auto& entry : std::filesystem::directory_iterator(disk_path_)) {
            if (entry.is_regular_file()) {
                auto last_write = std::filesystem::last_write_time(entry);
                auto time_point = std::chrono::system_clock::from_time_t(
                    std::chrono::duration_cast<std::chrono::seconds>(
                        last_write.time_since_epoch()
                    ).count()
                );
                
                if (is_expired(time_point)) {
                    disk_usage_ -= std::filesystem::file_size(entry);
                    std::filesystem::remove(entry);
                }
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Failed to cleanup disk: " << e.what() << std::endl;
    }
}

// Database storage implementations (placeholders)
bool PersistenceManager::store_database(const std::string& /*key*/, const std::vector<uint8_t>& /*data*/) {
    // Placeholder for database implementation
    std::cerr << "Database storage not implemented" << std::endl;
    return false;
}

std::optional<std::vector<uint8_t>> PersistenceManager::retrieve_database(const std::string& /*key*/) const {
    // Placeholder for database implementation
    return std::nullopt;
}

void PersistenceManager::cleanup_database() {
    // Placeholder for database implementation
}

// Helper functions
std::string PersistenceManager::get_disk_path(const std::string& key) const {
    return disk_path_ + "/" + key;
}

bool PersistenceManager::is_expired(const std::chrono::system_clock::time_point& timestamp) const {
    auto now = std::chrono::system_clock::now();
    auto age = now - timestamp;
    return age > config_.retention_hours;
}

} // namespace flashtick
