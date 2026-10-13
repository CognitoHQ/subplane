#pragma once

#include "../plugin_manager.h"
#include <librdkafka/rdkafka.h>
#include <string>
#include <vector>
#include <unordered_set>
#include <mutex>
#include <thread>
#include <atomic>

namespace subplane {

class KafkaProducer : public ProducerPlugin {
public:
    KafkaProducer();
    ~KafkaProducer() override;

    // Plugin interface
    std::string get_name() const override { return "kafka_producer"; }
    std::string get_version() const override { return "1.0.0"; }
    bool initialize(const std::string& config) override;
    void shutdown() override;

    // Transport interface
    bool connect() override;
    void disconnect() override;
    bool is_connected() const override;
    bool subscribe(const std::string& topic) override;
    bool unsubscribe(const std::string& topic) override;
    std::vector<std::string> get_subscribed_topics() const override;
    void set_message_callback(std::function<void(const std::string&, const std::vector<uint8_t>&)> callback) override;

private:
    struct KafkaConfig {
        std::string bootstrap_servers = "localhost:9092";
        std::string group_id = "subplane";
        std::string client_id = "subplane-client";
        std::string security_protocol = "plaintext";
        std::string sasl_mechanism;
        std::string sasl_username;
        std::string sasl_password;
        std::string ssl_ca_location;
        std::string ssl_certificate_location;
        std::string ssl_key_location;
        int session_timeout_ms = 30000;
        int heartbeat_interval_ms = 3000;
        bool enable_auto_commit = true;
        int auto_commit_interval_ms = 5000;
        std::string auto_offset_reset = "latest";
    };

    KafkaConfig config_;
    rd_kafka_t* producer_ = nullptr;
    rd_kafka_t* consumer_ = nullptr;
    rd_kafka_topic_partition_list_t* subscription_ = nullptr;
    
    std::function<void(const std::string&, const std::vector<uint8_t>&)> message_callback_;
    std::unordered_set<std::string> subscribed_topics_;
    
    std::atomic<bool> connected_;
    std::atomic<bool> running_;
    std::thread consumer_thread_;
    
    mutable std::mutex mutex_;
    
    void consumer_loop();
    static void message_callback(rd_kafka_t* rk, const rd_kafka_message_t* rkmessage, void* opaque);
    static void error_callback(rd_kafka_t* rk, int err, const char* reason, void* opaque);
    
    bool parse_config(const std::string& config_str);
    rd_kafka_conf_t* create_producer_config();
    rd_kafka_conf_t* create_consumer_config();
    void cleanup();
};

} // namespace subplane
