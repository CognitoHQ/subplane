#include "subplane/producer/kafka_producer.h"

extern "C" {
    subplane::Plugin* create_plugin() {
        return new subplane::KafkaProducer();
    }
}
