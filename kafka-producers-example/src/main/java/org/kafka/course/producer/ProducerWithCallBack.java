package org.kafka.course.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.kafka.course.configuration.KafkaProducerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallBack {

    private static final String TOPIC_NAME = "first-topic";
    private static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private KafkaProducer<String, String> kafkaProducer;

    public ProducerWithCallBack() {
        Properties properties = KafkaProducerConfiguration.getInstance().producerProperties();
        kafkaProducer = new KafkaProducer<>(properties);
    }

    public void send(String message) {
        logger.info("Send message: {}", message);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, message);
        kafkaProducer.send(record, (recordMetadata, e) -> {
            // executes every time a record is successfully sent or an exception is thrown
            if (e == null) {
                logger.info(
                        "Received new MetaData. \nTopic: {} \nPartition: {} \nOffset: {} \nTimestamp: {}",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp()
                );
            } else {
                logger.error("Error while producing message", e);
            }
        });
        kafkaProducer.flush();
    }

    public void close() {
        logger.info("Close Kafka with Call Back producer.");
        kafkaProducer.close();
    }
}
