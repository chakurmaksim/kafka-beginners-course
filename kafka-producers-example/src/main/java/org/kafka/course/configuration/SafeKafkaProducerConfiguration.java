package org.kafka.course.configuration;

import org.apache.kafka.clients.producer.ProducerConfig;
import java.util.Properties;

public class SafeKafkaProducerConfiguration {

    private static SafeKafkaProducerConfiguration instance;
    private Properties producerProperties;

    private SafeKafkaProducerConfiguration() {
        producerProperties = KafkaProducerConfiguration.getInstance().producerProperties();
        producerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        producerProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
    }

    public static SafeKafkaProducerConfiguration getInstance() {
        if (instance == null) {
            instance = new SafeKafkaProducerConfiguration();
        }
        return instance;
    }

    public Properties producerProperties() {
        return producerProperties;
    }
}
