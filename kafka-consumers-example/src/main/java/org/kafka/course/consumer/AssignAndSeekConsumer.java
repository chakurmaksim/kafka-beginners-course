package org.kafka.course.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.kafka.course.configuration.KafkaConsumerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class AssignAndSeekConsumer {

    private static final String TOPIC_NAME = "first-topic";
    private static Logger logger = LoggerFactory.getLogger(AssignAndSeekConsumer.class);
    private KafkaConsumer<String, String> kafkaConsumer;

    public AssignAndSeekConsumer(int partition, long offsetToReadFrom) {
        Properties properties = KafkaConsumerConfiguration.getInstance().consumerProperties();
        kafkaConsumer = new KafkaConsumer<>(properties);
        TopicPartition partitionToReadFrom = new TopicPartition(TOPIC_NAME, partition);
        kafkaConsumer.assign(Arrays.asList(partitionToReadFrom));
        kafkaConsumer.seek(partitionToReadFrom, offsetToReadFrom);
    }

    public void receive(int numberOfMessagesToRead) {
        boolean keepOnReading = true;
        int numberOfMessagesWereRead = 0;
        while (keepOnReading) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                if (numberOfMessagesWereRead < numberOfMessagesToRead) {
                    logger.info("Received Message, Key: {} to Value: {}", record.key(), record.value());
                    logger.info("Message with the Key: {} from Partition: {} and Offset: {}",
                            record.key(), record.partition(), record.offset());
                } else {
                    keepOnReading = false;
                    break;
                }
                numberOfMessagesWereRead += 1;
            }
        }
        logger.info("Close AssignAndSeek Kafka consumer.");
        kafkaConsumer.close();
    }
}
