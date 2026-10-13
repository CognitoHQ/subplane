#include "subplane/metrics/metrics_manager.h"

#include <fstream>
#include <iostream>
#include <yaml-cpp/yaml.h>

namespace subplane {

MetricsManager::MetricsManager() : running_(false) {}

MetricsManager::~MetricsManager() {
    stop();
}

bool MetricsManager::initialize(const std::string& config) {
    return parse_config(config);
}

bool MetricsManager::start() {
    if (running_.load()) {
        return false;
    }
    if (!config_.enabled) {
        return true;
    }
    running_.store(true);
    metrics_thread_ = std::thread(&MetricsManager::metrics_loop, this);
    return true;
}

void MetricsManager::stop() {
    if (!running_.load()) {
        return;
    }
    running_.store(false);
    if (metrics_thread_.joinable()) {
        metrics_thread_.join();
    }
}

void MetricsManager::set_provider(Provider provider) {
    std::lock_guard<std::mutex> lock(mutex_);
    provider_ = std::move(provider);
}

void MetricsManager::set_metrics_callback(std::function<void(const std::string&)> callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    metrics_callback_ = std::move(callback);
}

std::string MetricsManager::get_metrics_json() const {
    return render("json");
}

std::string MetricsManager::get_summary() const {
    return render("text");
}

std::string MetricsManager::render(const std::string& format) const {
    Provider provider;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        provider = provider_;
    }
    if (!provider) {
        return format == "json" ? "{}" : "";
    }
    return provider(format);
}

void MetricsManager::metrics_loop() {
    // Sleep in small steps so stop() is responsive.
    const auto interval = std::chrono::seconds(config_.report_interval_seconds);
    auto next = std::chrono::steady_clock::now() + interval;
    while (running_.load()) {
        if (std::chrono::steady_clock::now() < next) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            continue;
        }
        next += interval;
        try {
            std::string metrics = render(config_.output_format);
            if (config_.console_output) {
                std::cout << metrics << std::endl;
            }
            if (!config_.output_file.empty()) {
                write_metrics_to_file(metrics);
            }
            std::function<void(const std::string&)> cb;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                cb = metrics_callback_;
            }
            if (cb) {
                cb(metrics);
            }
        } catch (const std::exception& e) {
            std::cerr << "metrics loop error: " << e.what() << std::endl;
        }
    }
}

bool MetricsManager::parse_config(const std::string& config_str) {
    try {
        YAML::Node config = YAML::Load(config_str);
        config_.enabled = config["enabled"].as<bool>(true);
        config_.report_interval_seconds = config["report_interval_seconds"].as<int>(60);
        config_.output_format = config["output_format"].as<std::string>("json");
        config_.output_file = config["output_file"].as<std::string>("");
        config_.console_output = config["console_output"].as<bool>(false);
        if (config_.report_interval_seconds <= 0) {
            config_.report_interval_seconds = 60;
        }
        return true;
    } catch (const YAML::Exception& e) {
        std::cerr << "metrics config error: " << e.what() << std::endl;
        return false;
    }
}

void MetricsManager::write_metrics_to_file(const std::string& metrics) const {
    std::ofstream file(config_.output_file, std::ios::app);
    if (!file.is_open()) {
        std::cerr << "failed to open metrics file: " << config_.output_file << std::endl;
        return;
    }
    file << metrics << "\n";
}

} // namespace subplane
