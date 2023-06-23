package org.kafka.course.configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaConsumerConfiguration {

    private static final String CONSUMER_GROUP_ID = "simple-kafka-consumer";
    private static KafkaConsumerConfiguration instance;
    private Properties consumerProperties;

    private KafkaConsumerConfiguration() {
        String kafkaUri = CommonKafkaConfiguration.getInstance().getKafkaUriSting();
        consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUri);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    public static KafkaConsumerConfiguration getInstance() {
        if (instance == null) {
            instance = new KafkaConsumerConfiguration();
        }
        return instance;
    }

    public Properties consumerProperties() {
        return consumerProperties;
    }
}
