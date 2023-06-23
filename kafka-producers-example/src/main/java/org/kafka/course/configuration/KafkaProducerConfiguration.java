package org.kafka.course.configuration;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerConfiguration {

    private static KafkaProducerConfiguration instance;
    private Properties producerProperties;

    private KafkaProducerConfiguration() {
        String kafkaUri = CommonKafkaConfiguration.getInstance().getKafkaUriSting();
        producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUri);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    public static KafkaProducerConfiguration getInstance() {
        if (instance == null) {
            instance = new KafkaProducerConfiguration();
        }
        return instance;
    }

    public Properties producerProperties() {
        return producerProperties;
    }
}
