package org.kafka.course.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.kafka.course.configuration.KafkaProducerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKeys {

    private static final String TOPIC_NAME = "first-topic";
    private static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private KafkaProducer<String, String> kafkaProducer;

    public ProducerWithKeys() {
        Properties properties = KafkaProducerConfiguration.getInstance().producerProperties();
        kafkaProducer = new KafkaProducer<>(properties);
    }

    public void send(String message) throws ExecutionException, InterruptedException {
        for (int i = 0; i < 10; i++) {
            String modifiedMessage = message + " " + i;
            String key = "id_" + i;
            logger.info("Send message: {} for Key: {}", modifiedMessage, key);
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, modifiedMessage);
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
            }).get(); // block the .send() to make it synchronous - don't do this in Production
        }
        kafkaProducer.flush();
    }

    public void close() {
        logger.info("Close Kafka with Call Back producer.");
        kafkaProducer.close();
    }
}
