package org.kafka.course.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.kafka.course.configuration.KafkaConsumerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;

public class SimpleConsumer {

    private static final String TOPIC_NAME = "first-topic";
    private static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private KafkaConsumer<String, String> kafkaConsumer;


    public SimpleConsumer() {
        Properties properties = KafkaConsumerConfiguration.getInstance().consumerProperties();
        kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton(TOPIC_NAME)); // Subscribe consumer to topic(s)
    }

    public void receive() {
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            consumerRecords.forEach(
                    new Consumer<ConsumerRecord<String, String>>() {
                        @Override
                        public void accept(ConsumerRecord<String, String> record) {
                            logger.info("Received Message, Key: {} to Value: {}", record.key(), record.value());
                            logger.info("Message with the Key: {} from Partition: {} and Offset: {}",
                                    record.key(), record.partition(), record.offset());
                        }
                    }
            );
        }
    }

    public void close() {
        logger.info("Close Simple Kafka consumer.");
        kafkaConsumer.close();
    }
}
