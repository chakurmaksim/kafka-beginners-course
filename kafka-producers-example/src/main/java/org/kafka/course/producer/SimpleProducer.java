package org.kafka.course.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.kafka.course.configuration.KafkaProducerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducer {

    private static final String TOPIC_NAME = "first-topic";
    private static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private KafkaProducer<String, String> kafkaProducer;


    public SimpleProducer() {
        Properties properties = KafkaProducerConfiguration.getInstance().producerProperties();
        kafkaProducer = new KafkaProducer<>(properties);
    }

    public void send(String message) {
        logger.info("Send message: {}", message);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, message);
        kafkaProducer.send(record);
        kafkaProducer.flush();
    }

    public void close() {
        logger.info("Close Simple Kafka producer.");
        kafkaProducer.close();
    }
}
