#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <optional>
#include <chrono>
#include <mutex>

namespace subplane {

struct PersistenceConfig {
    enum class Type {
        MEMORY,
        DISK,
        DATABASE
    };
    
    Type type = Type::MEMORY;
    std::string path;  // For disk/database
    size_t max_memory_size = 1024 * 1024 * 100;  // 100MB
    size_t max_disk_size = 1024 * 1024 * 1024;   // 1GB
    std::chrono::hours retention_hours{24};
};

class PersistenceManager {
public:
    PersistenceManager();
    ~PersistenceManager();

    // Initialize with configuration
    bool initialize(const PersistenceConfig& config);
    
    // Store data
    bool store(const std::string& key, const std::vector<uint8_t>& data);
    
    // Retrieve data
    std::optional<std::vector<uint8_t>> retrieve(const std::string& key) const;
    
    // Check if key exists
    bool exists(const std::string& key) const;
    
    // Remove data
    bool remove(const std::string& key);
    
    // List all keys
    std::vector<std::string> list_keys() const;
    
    // Get storage statistics
    struct Stats {
        size_t total_keys;
        size_t total_size;
        size_t memory_usage;
        size_t disk_usage;
    };
    Stats get_stats() const;
    
    // Cleanup old data
    void cleanup();
    
    // Shutdown
    void shutdown();

private:
    PersistenceConfig config_;
    
    // Memory storage
    struct MemoryEntry {
        std::vector<uint8_t> data;
        std::chrono::system_clock::time_point timestamp;
    };
    mutable std::mutex memory_mutex_;
    std::unordered_map<std::string, MemoryEntry> memory_storage_;
    size_t memory_usage_ = 0;
    
    // Disk storage
    mutable std::mutex disk_mutex_;
    std::string disk_path_;
    size_t disk_usage_ = 0;
    
    // Database storage (placeholder for future implementation)
    mutable std::mutex db_mutex_;
    
    void cleanup_memory();
    void cleanup_disk();
    void cleanup_database();
    
    bool store_memory(const std::string& key, const std::vector<uint8_t>& data);
    bool store_disk(const std::string& key, const std::vector<uint8_t>& data);
    bool store_database(const std::string& key, const std::vector<uint8_t>& data);
    
    std::optional<std::vector<uint8_t>> retrieve_memory(const std::string& key) const;
    std::optional<std::vector<uint8_t>> retrieve_disk(const std::string& key) const;
    std::optional<std::vector<uint8_t>> retrieve_database(const std::string& key) const;
    
    std::string get_disk_path(const std::string& key) const;
    bool is_expired(const std::chrono::system_clock::time_point& timestamp) const;
};

} // namespace subplane
