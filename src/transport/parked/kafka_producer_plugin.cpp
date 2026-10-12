#include "flashtick/producer/kafka_producer.h"

extern "C" {
    flashtick::Plugin* create_plugin() {
        return new flashtick::KafkaProducer();
    }
}
