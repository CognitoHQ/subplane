#pragma once

#include "../types.h"
#include <vector>
#include <memory>
#include <mutex>
#include <atomic>
#include <chrono>
#include <thread>
#include <functional>
#include <fstream>
#include <sstream>
#include <iomanip>

namespace flashtick {

// Log levels
enum class LogLevel {
    TRACE = 0,
    DEBUG = 1,
    INFO = 2,
    WARN = 3,
    ERROR = 4,
    FATAL = 5
};

// Convert log level to string
inline LogLevelType log_level_to_string(LogLevel level) {
    switch (level) {
        case LogLevel::TRACE: return "TRACE";
        case LogLevel::DEBUG: return "DEBUG";
        case LogLevel::INFO:  return "INFO";
        case LogLevel::WARN:  return "WARN";
        case LogLevel::ERROR: return "ERROR";
        case LogLevel::FATAL: return "FATAL";
        default: return "UNKNOWN";
    }
}

// Convert string to log level
inline LogLevel string_to_log_level(const LogLevelType& level_str) {
    if (level_str == "TRACE") return LogLevel::TRACE;
    if (level_str == "DEBUG") return LogLevel::DEBUG;
    if (level_str == "INFO")  return LogLevel::INFO;
    if (level_str == "WARN")  return LogLevel::WARN;
    if (level_str == "ERROR") return LogLevel::ERROR;
    if (level_str == "FATAL") return LogLevel::FATAL;
    return LogLevel::INFO; // Default
}

// Log entry structure
struct LogEntry {
    TimestampType timestamp;
    LogLevel level;
    ComponentNameType component;
    EventNameType event;
    LogMessageType message;
    ThreadIdType thread_id;
    MetadataType metadata;
    
    LogEntry() = default;
    
    LogEntry(LogLevel lvl, const ComponentNameType& comp, const EventNameType& evt, 
             const LogMessageType& msg, const MetadataType& meta = {})
        : timestamp(std::chrono::system_clock::now())
        , level(lvl)
        , component(comp)
        , event(evt)
        , message(msg)
        , thread_id(std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())))
        , metadata(meta) {}
    
    StringType to_string() const {
        std::ostringstream oss;
        auto time_t = std::chrono::system_clock::to_time_t(timestamp);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            timestamp.time_since_epoch()) % 1000;
        
        oss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
        oss << "." << std::setfill('0') << std::setw(3) << ms.count();
        oss << " [" << log_level_to_string(level) << "]";
        oss << " [" << component << "]";
        oss << " [" << event << "]";
        oss << " " << message;
        
        if (!metadata.empty()) {
            oss << " {";
            bool first = true;
            for (const auto& [key, value] : metadata) {
                if (!first) oss << ", ";
                oss << key << "=" << value;
                first = false;
            }
            oss << "}";
        }
        
        return oss.str();
    }
    
    StringType to_json() const {
        std::ostringstream oss;
        auto time_t = std::chrono::system_clock::to_time_t(timestamp);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            timestamp.time_since_epoch()) % 1000;
        
        oss << "{";
        oss << "\"timestamp\":\"" << std::put_time(std::localtime(&time_t), "%Y-%m-%dT%H:%M:%S");
        oss << "." << std::setfill('0') << std::setw(3) << ms.count() << "Z\",";
        oss << "\"level\":\"" << log_level_to_string(level) << "\",";
        oss << "\"component\":\"" << component << "\",";
        oss << "\"event\":\"" << event << "\",";
        oss << "\"message\":\"" << message << "\",";
        oss << "\"thread_id\":\"" << thread_id << "\"";
        
        if (!metadata.empty()) {
            oss << ",\"metadata\":{";
            bool first = true;
            for (const auto& [key, value] : metadata) {
                if (!first) oss << ",";
                oss << "\"" << key << "\":\"" << value << "\"";
                first = false;
            }
            oss << "}";
        }
        
        oss << "}";
        return oss.str();
    }
};

// Log registry for tracking important events
class LogRegistry {
public:
    static LogRegistry& instance() {
        static LogRegistry instance;
        return instance;
    }
    
    // Initialize the log registry
    bool initialize(const ConfigType& config);
    
    // Log an event
    void log(LogLevel level, const ComponentNameType& component, const EventNameType& event, 
             const LogMessageType& message, const MetadataType& metadata = {});
    
    // Convenience methods for common log levels
    void trace(const ComponentNameType& component, const EventNameType& event, const LogMessageType& message,
               const MetadataType& metadata = {}) {
        log(LogLevel::TRACE, component, event, message, metadata);
    }
    
    void debug(const ComponentNameType& component, const EventNameType& event, const LogMessageType& message,
               const MetadataType& metadata = {}) {
        log(LogLevel::DEBUG, component, event, message, metadata);
    }
    
    void info(const ComponentNameType& component, const EventNameType& event, const LogMessageType& message,
              const MetadataType& metadata = {}) {
        log(LogLevel::INFO, component, event, message, metadata);
    }
    
    void warn(const ComponentNameType& component, const EventNameType& event, const LogMessageType& message,
              const MetadataType& metadata = {}) {
        log(LogLevel::WARN, component, event, message, metadata);
    }
    
    void error(const ComponentNameType& component, const EventNameType& event, const LogMessageType& message,
               const MetadataType& metadata = {}) {
        log(LogLevel::ERROR, component, event, message, metadata);
    }
    
    void fatal(const ComponentNameType& component, const EventNameType& event, const LogMessageType& message,
               const MetadataType& metadata = {}) {
        log(LogLevel::FATAL, component, event, message, metadata);
    }
    
    // Process lifecycle events
    void process_started(const StringType& process_name, const StringType& pid = "") {
        MetadataType meta;
        if (!pid.empty()) meta["pid"] = pid;
        info("PROCESS", "STARTED", "Process started: " + process_name, meta);
    }
    
    void process_stopped(const StringType& process_name, const StringType& pid = "", 
                        const StringType& reason = "") {
        MetadataType meta;
        if (!pid.empty()) meta["pid"] = pid;
        if (!reason.empty()) meta["reason"] = reason;
        info("PROCESS", "STOPPED", "Process stopped: " + process_name, meta);
    }
    
    // Thread lifecycle events
    void thread_started(const StringType& thread_name, const ThreadIdType& thread_id = "") {
        MetadataType meta;
        if (!thread_id.empty()) meta["thread_id"] = thread_id;
        info("THREAD", "STARTED", "Thread started: " + thread_name, meta);
    }
    
    void thread_stopped(const StringType& thread_name, const ThreadIdType& thread_id = "") {
        MetadataType meta;
        if (!thread_id.empty()) meta["thread_id"] = thread_id;
        info("THREAD", "STOPPED", "Thread stopped: " + thread_name, meta);
    }
    
    // Task lifecycle events
    void task_started(const std::string& task_name, const std::string& task_id = "") {
        std::unordered_map<std::string, std::string> meta;
        if (!task_id.empty()) meta["task_id"] = task_id;
        info("TASK", "STARTED", "Task started: " + task_name, meta);
    }
    
    void task_completed(const std::string& task_name, const std::string& task_id = "", 
                       const std::string& duration_ms = "") {
        std::unordered_map<std::string, std::string> meta;
        if (!task_id.empty()) meta["task_id"] = task_id;
        if (!duration_ms.empty()) meta["duration_ms"] = duration_ms;
        info("TASK", "COMPLETED", "Task completed: " + task_name, meta);
    }
    
    void task_failed(const std::string& task_name, const std::string& task_id = "", 
                    const std::string& error_msg = "") {
        std::unordered_map<std::string, std::string> meta;
        if (!task_id.empty()) meta["task_id"] = task_id;
        if (!error_msg.empty()) meta["error"] = error_msg;
        error("TASK", "FAILED", "Task failed: " + task_name, meta);
    }
    
    // Connection events
    void connection_established(const std::string& connection_type, const std::string& endpoint = "") {
        std::unordered_map<std::string, std::string> meta;
        if (!endpoint.empty()) meta["endpoint"] = endpoint;
        info("CONNECTION", "ESTABLISHED", "Connection established: " + connection_type, meta);
    }
    
    void connection_lost(const std::string& connection_type, const std::string& endpoint = "", 
                        const std::string& reason = "") {
        std::unordered_map<std::string, std::string> meta;
        if (!endpoint.empty()) meta["endpoint"] = endpoint;
        if (!reason.empty()) meta["reason"] = reason;
        warn("CONNECTION", "LOST", "Connection lost: " + connection_type, meta);
    }
    
    // Plugin events
    void plugin_loaded(const std::string& plugin_name, const std::string& plugin_path = "") {
        std::unordered_map<std::string, std::string> meta;
        if (!plugin_path.empty()) meta["path"] = plugin_path;
        info("PLUGIN", "LOADED", "Plugin loaded: " + plugin_name, meta);
    }
    
    void plugin_unloaded(const std::string& plugin_name) {
        info("PLUGIN", "UNLOADED", "Plugin unloaded: " + plugin_name);
    }
    
    void plugin_failed(const std::string& plugin_name, const std::string& error_msg = "") {
        std::unordered_map<std::string, std::string> meta;
        if (!error_msg.empty()) meta["error"] = error_msg;
        error("PLUGIN", "FAILED", "Plugin failed: " + plugin_name, meta);
    }
    
    // Configuration events
    void config_loaded(const std::string& config_path) {
        std::unordered_map<std::string, std::string> meta;
        meta["path"] = config_path;
        info("CONFIG", "LOADED", "Configuration loaded", meta);
    }
    
    void config_reloaded(const std::string& config_path) {
        std::unordered_map<std::string, std::string> meta;
        meta["path"] = config_path;
        info("CONFIG", "RELOADED", "Configuration reloaded", meta);
    }
    
    // Message processing events
    void message_received(const std::string& topic, const std::string& source = "", 
                         const std::string& size_bytes = "") {
        std::unordered_map<std::string, std::string> meta;
        meta["topic"] = topic;
        if (!source.empty()) meta["source"] = source;
        if (!size_bytes.empty()) meta["size_bytes"] = size_bytes;
        debug("MESSAGE", "RECEIVED", "Message received on topic: " + topic, meta);
    }
    
    void message_processed(const std::string& topic, const std::string& processing_time_ms = "") {
        std::unordered_map<std::string, std::string> meta;
        meta["topic"] = topic;
        if (!processing_time_ms.empty()) meta["processing_time_ms"] = processing_time_ms;
        debug("MESSAGE", "PROCESSED", "Message processed on topic: " + topic, meta);
    }
    
    // Get recent log entries
    std::vector<LogEntry> get_recent_entries(size_t count = 100) const;
    
    // Get log entries by component
    std::vector<LogEntry> get_entries_by_component(const std::string& component, size_t count = 100) const;
    
    // Get log entries by event
    std::vector<LogEntry> get_entries_by_event(const std::string& event, size_t count = 100) const;
    
    // Get log entries by level
    std::vector<LogEntry> get_entries_by_level(LogLevel level, size_t count = 100) const;
    
    // Set log level filter
    void set_log_level(LogLevel level) { min_log_level_ = level; }
    
    // Set output callback
    void set_output_callback(std::function<void(const LogEntry&)> callback);
    
    // Clear all logs
    void clear();
    
    // Shutdown
    void shutdown();

private:
    LogRegistry() = default;
    ~LogRegistry() = default;
    
    struct Config {
        bool enabled = true;
        LogLevel min_level = LogLevel::INFO;
        size_t max_entries = 10000;
        bool console_output = true;
        std::string log_file = "";
        std::string output_format = "text"; // text, json
        bool include_metadata = true;
    };
    
    Config config_;
    std::atomic<LogLevel> min_log_level_;
    std::vector<LogEntry> log_entries_;
    mutable std::mutex entries_mutex_;
    
    std::function<void(const LogEntry&)> output_callback_;
    mutable std::mutex callback_mutex_;
    
    std::ofstream log_file_;
    mutable std::mutex file_mutex_;
    
    bool parse_config(const std::string& config_str);
    void write_to_console(const LogEntry& entry) const;
    void write_to_file(const LogEntry& entry);
    void call_output_callback(const LogEntry& entry);
    void trim_entries();
};

// Convenience macros for logging
#define FLASHTICK_LOG_TRACE(component, event, message) \
    flashtick::LogRegistry::instance().trace(component, event, message)

#define FLASHTICK_LOG_DEBUG(component, event, message) \
    flashtick::LogRegistry::instance().debug(component, event, message)

#define FLASHTICK_LOG_INFO(component, event, message) \
    flashtick::LogRegistry::instance().info(component, event, message)

#define FLASHTICK_LOG_WARN(component, event, message) \
    flashtick::LogRegistry::instance().warn(component, event, message)

#define FLASHTICK_LOG_ERROR(component, event, message) \
    flashtick::LogRegistry::instance().error(component, event, message)

#define FLASHTICK_LOG_FATAL(component, event, message) \
    flashtick::LogRegistry::instance().fatal(component, event, message)

#define FLASHTICK_LOG_TRACE_WITH_META(component, event, message, metadata) \
    flashtick::LogRegistry::instance().trace(component, event, message, metadata)

#define FLASHTICK_LOG_DEBUG_WITH_META(component, event, message, metadata) \
    flashtick::LogRegistry::instance().debug(component, event, message, metadata)

#define FLASHTICK_LOG_INFO_WITH_META(component, event, message, metadata) \
    flashtick::LogRegistry::instance().info(component, event, message, metadata)

#define FLASHTICK_LOG_WARN_WITH_META(component, event, message, metadata) \
    flashtick::LogRegistry::instance().warn(component, event, message, metadata)

#define FLASHTICK_LOG_ERROR_WITH_META(component, event, message, metadata) \
    flashtick::LogRegistry::instance().error(component, event, message, metadata)

#define FLASHTICK_LOG_FATAL_WITH_META(component, event, message, metadata) \
    flashtick::LogRegistry::instance().fatal(component, event, message, metadata)

} // namespace flashtick
