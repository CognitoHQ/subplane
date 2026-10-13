#include "subplane/producer/kafka_producer.h"
#include <iostream>
#include <sstream>
#include <yaml-cpp/yaml.h>

namespace subplane {

KafkaProducer::KafkaProducer() : connected_(false), running_(false) {
}

KafkaProducer::~KafkaProducer() {
    shutdown();
}

bool KafkaProducer::initialize(const std::string& config) {
    try {
        if (!parse_config(config)) {
            std::cerr << "Failed to parse Kafka configuration" << std::endl;
            return false;
        }
        
        std::cout << "Kafka transport initialized" << std::endl;
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception during Kafka transport initialization: " << e.what() << std::endl;
        return false;
    }
}

void KafkaProducer::shutdown() {
    running_.store(false);
    cleanup();
}

bool KafkaProducer::connect() {
    if (connected_.load()) {
        return true;
    }
    
    try {
        // Create producer
        rd_kafka_conf_t* producer_conf = create_producer_config();
        if (!producer_conf) {
            return false;
        }
        
        char errstr[512];
        producer_ = rd_kafka_new(RD_KAFKA_PRODUCER, producer_conf, errstr, sizeof(errstr));
        if (!producer_) {
            std::cerr << "Failed to create producer: " << errstr << std::endl;
            return false;
        }
        
        // Create consumer
        rd_kafka_conf_t* consumer_conf = create_consumer_config();
        if (!consumer_conf) {
            return false;
        }
        
        consumer_ = rd_kafka_new(RD_KAFKA_CONSUMER, consumer_conf, errstr, sizeof(errstr));
        if (!consumer_) {
            std::cerr << "Failed to create consumer: " << errstr << std::endl;
            return false;
        }
        
        // Add brokers
        if (rd_kafka_brokers_add(producer_, config_.bootstrap_servers.c_str()) == 0) {
            std::cerr << "No brokers added to producer" << std::endl;
            return false;
        }
        
        if (rd_kafka_brokers_add(consumer_, config_.bootstrap_servers.c_str()) == 0) {
            std::cerr << "No brokers added to consumer" << std::endl;
            return false;
        }
        
        connected_.store(true);
        std::cout << "Connected to Kafka brokers: " << config_.bootstrap_servers << std::endl;
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception during Kafka connection: " << e.what() << std::endl;
        return false;
    }
}

void KafkaProducer::disconnect() {
    if (!connected_.load()) {
        return;
    }
    
    running_.store(false);
    cleanup();
    connected_.store(false);
    
    std::cout << "Disconnected from Kafka" << std::endl;
}

bool KafkaProducer::is_connected() const {
    return connected_.load();
}

bool KafkaProducer::subscribe(const std::string& topic) {
    if (!connected_.load()) {
        std::cerr << "Not connected to Kafka" << std::endl;
        return false;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Add topic to subscription list
    if (!subscription_) {
        subscription_ = rd_kafka_topic_partition_list_new(1);
    }
    
    rd_kafka_topic_partition_list_add(subscription_, topic.c_str(), RD_KAFKA_PARTITION_UA);
    
    // Subscribe to topics
    rd_kafka_resp_err_t err = rd_kafka_subscribe(consumer_, subscription_);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        std::cerr << "Failed to subscribe to topic " << topic << ": " << rd_kafka_err2str(err) << std::endl;
        return false;
    }
    
    subscribed_topics_.insert(topic);
    std::cout << "Subscribed to Kafka topic: " << topic << std::endl;
    
    // Start consumer thread if not running
    if (!running_.load()) {
        running_.store(true);
        consumer_thread_ = std::thread(&KafkaProducer::consumer_loop, this);
    }
    
    return true;
}

bool KafkaProducer::unsubscribe(const std::string& topic) {
    if (!connected_.load()) {
        return true;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    subscribed_topics_.erase(topic);
    
    // Rebuild subscription list
    if (subscription_) {
        rd_kafka_topic_partition_list_destroy(subscription_);
        subscription_ = nullptr;
    }
    
    if (!subscribed_topics_.empty()) {
        subscription_ = rd_kafka_topic_partition_list_new(subscribed_topics_.size());
        for (const auto& t : subscribed_topics_) {
            rd_kafka_topic_partition_list_add(subscription_, t.c_str(), RD_KAFKA_PARTITION_UA);
        }
        
        rd_kafka_resp_err_t err = rd_kafka_subscribe(consumer_, subscription_);
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            std::cerr << "Failed to update subscription: " << rd_kafka_err2str(err) << std::endl;
            return false;
        }
    } else {
        rd_kafka_resp_err_t err = rd_kafka_unsubscribe(consumer_);
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            std::cerr << "Failed to unsubscribe: " << rd_kafka_err2str(err) << std::endl;
            return false;
        }
    }
    
    std::cout << "Unsubscribed from Kafka topic: " << topic << std::endl;
    return true;
}

std::vector<std::string> KafkaProducer::get_subscribed_topics() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return std::vector<std::string>(subscribed_topics_.begin(), subscribed_topics_.end());
}

void KafkaProducer::set_message_callback(std::function<void(const std::string&, const std::vector<uint8_t>&)> callback) {
    message_callback_ = callback;
}

void KafkaProducer::consumer_loop() {
    while (running_.load()) {
        rd_kafka_message_t* rkmessage = rd_kafka_consumer_poll(consumer_, 1000);
        
        if (rkmessage) {
            if (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                // Process message
                std::string topic = rd_kafka_topic_name(rkmessage->rkt);
                std::vector<uint8_t> data(
                    static_cast<uint8_t*>(rkmessage->payload),
                    static_cast<uint8_t*>(rkmessage->payload) + rkmessage->len
                );
                
                if (message_callback_) {
                    message_callback_(topic, data);
                }
            } else {
                std::cerr << "Consumer error: " << rd_kafka_message_errstr(rkmessage) << std::endl;
            }
            
            rd_kafka_message_destroy(rkmessage);
        }
    }
}

void KafkaProducer::message_callback(rd_kafka_t* rk, const rd_kafka_message_t* rkmessage, void* opaque) {
    // This is a static callback, implementation would need to route to instance
    // For now, we handle messages in the consumer loop
}

void KafkaProducer::error_callback(rd_kafka_t* rk, int err, const char* reason, void* opaque) {
    std::cerr << "Kafka error: " << rd_kafka_err2str(static_cast<rd_kafka_resp_err_t>(err)) 
              << ": " << reason << std::endl;
}

bool KafkaProducer::parse_config(const std::string& config_str) {
    try {
        YAML::Node config = YAML::Load(config_str);
        
        config_.bootstrap_servers = config["bootstrap_servers"].as<std::string>("localhost:9092");
        config_.group_id = config["group_id"].as<std::string>("subplane");
        config_.client_id = config["client_id"].as<std::string>("subplane-client");
        config_.security_protocol = config["security_protocol"].as<std::string>("plaintext");
        config_.session_timeout_ms = config["session_timeout_ms"].as<int>(30000);
        config_.heartbeat_interval_ms = config["heartbeat_interval_ms"].as<int>(3000);
        config_.enable_auto_commit = config["enable_auto_commit"].as<bool>(true);
        config_.auto_commit_interval_ms = config["auto_commit_interval_ms"].as<int>(5000);
        config_.auto_offset_reset = config["auto_offset_reset"].as<std::string>("latest");
        
        return true;
        
    } catch (const YAML::Exception& e) {
        std::cerr << "YAML parsing error: " << e.what() << std::endl;
        return false;
    }
}

rd_kafka_conf_t* KafkaProducer::create_producer_config() {
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    
    // Set basic configuration
    rd_kafka_conf_set(conf, "bootstrap.servers", config_.bootstrap_servers.c_str(), nullptr, 0);
    rd_kafka_conf_set(conf, "client.id", config_.client_id.c_str(), nullptr, 0);
    rd_kafka_conf_set(conf, "security.protocol", config_.security_protocol.c_str(), nullptr, 0);
    
    // Set error callback
    rd_kafka_conf_set_error_cb(conf, error_callback);
    
    return conf;
}

rd_kafka_conf_t* KafkaProducer::create_consumer_config() {
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    
    // Set basic configuration
    rd_kafka_conf_set(conf, "bootstrap.servers", config_.bootstrap_servers.c_str(), nullptr, 0);
    rd_kafka_conf_set(conf, "group.id", config_.group_id.c_str(), nullptr, 0);
    rd_kafka_conf_set(conf, "client.id", config_.client_id.c_str(), nullptr, 0);
    rd_kafka_conf_set(conf, "security.protocol", config_.security_protocol.c_str(), nullptr, 0);
    
    // Set session and heartbeat timeouts
    rd_kafka_conf_set(conf, "session.timeout.ms", std::to_string(config_.session_timeout_ms).c_str(), nullptr, 0);
    rd_kafka_conf_set(conf, "heartbeat.interval.ms", std::to_string(config_.heartbeat_interval_ms).c_str(), nullptr, 0);
    
    // Set auto commit settings
    rd_kafka_conf_set(conf, "enable.auto.commit", config_.enable_auto_commit ? "true" : "false", nullptr, 0);
    rd_kafka_conf_set(conf, "auto.commit.interval.ms", std::to_string(config_.auto_commit_interval_ms).c_str(), nullptr, 0);
    
    // Set offset reset
    rd_kafka_conf_set(conf, "auto.offset.reset", config_.auto_offset_reset.c_str(), nullptr, 0);
    
    // Set error callback
    rd_kafka_conf_set_error_cb(conf, error_callback);
    
    return conf;
}

void KafkaProducer::cleanup() {
    if (consumer_thread_.joinable()) {
        running_.store(false);
        consumer_thread_.join();
    }
    
    if (subscription_) {
        rd_kafka_topic_partition_list_destroy(subscription_);
        subscription_ = nullptr;
    }
    
    if (consumer_) {
        rd_kafka_consumer_close(consumer_);
        rd_kafka_destroy(consumer_);
        consumer_ = nullptr;
    }
    
    if (producer_) {
        rd_kafka_flush(producer_, 10000);
        rd_kafka_destroy(producer_);
        producer_ = nullptr;
    }
}

} // namespace subplane
