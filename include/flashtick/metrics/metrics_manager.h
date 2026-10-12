#pragma once

// Cold-path periodic metrics reporter. It owns no metrics itself: the Agent
// registers a provider that renders the current counters/histograms to JSON
// or text, and this thread calls it on an interval and writes it out.

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

namespace flashtick {

class MetricsManager {
public:
    // Renders a report. format is "json" or "text".
    using Provider = std::function<std::string(const std::string& format)>;

    MetricsManager();
    ~MetricsManager();

    // config is a YAML string (the `metrics` section).
    bool initialize(const std::string& config);
    bool start();
    void stop();
    bool is_running() const { return running_.load(); }

    // Thread-safe. The provider is called from the metrics thread and from
    // get_metrics_json() (any thread).
    void set_provider(Provider provider);

    // Thread-safe.
    void set_metrics_callback(std::function<void(const std::string&)> callback);

    // Renders now via the provider. Thread-safe.
    std::string get_metrics_json() const;
    std::string get_summary() const;

    bool enabled() const { return config_.enabled; }

private:
    struct MetricsConfig {
        bool enabled = true;
        int report_interval_seconds = 60;
        std::string output_format = "json"; // json, text
        std::string output_file;
        bool console_output = false;
    };

    MetricsConfig config_;
    std::atomic<bool> running_;
    std::thread metrics_thread_;

    mutable std::mutex mutex_;
    Provider provider_;
    std::function<void(const std::string&)> metrics_callback_;

    void metrics_loop();
    bool parse_config(const std::string& config_str);
    std::string render(const std::string& format) const;
    void write_metrics_to_file(const std::string& metrics) const;
};

} // namespace flashtick
