package org.kafka.course.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.kafka.course.configuration.KafkaConsumerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class ConsumerWithThread {

    private static Logger logger = LoggerFactory.getLogger(ConsumerWithThread.class);
    private ConsumerRunnable consumerRunnable;
    private Thread consumerThread;
    private CountDownLatch countDownLatch;

    public ConsumerWithThread() {
        consumerRunnable = new ConsumerRunnable();
        consumerThread = new Thread(consumerRunnable);
        this.countDownLatch = new CountDownLatch(1);
    }

    public void startListening() {
        consumerThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            stopListening();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                logger.error("Consumer With Thread was interrupted!", e);
            } finally {
                logger.info("Kafka consumer stopped listening.");
            }
        }));
    }

    public void stopListening() {
        consumerRunnable.shutdown();
    }

    private class ConsumerRunnable implements Runnable {

        private static final String TOPIC_NAME = "first-topic";
        private KafkaConsumer<String, String> kafkaConsumer;

        private ConsumerRunnable() {
            Properties properties = KafkaConsumerConfiguration.getInstance().consumerProperties();
            kafkaConsumer = new KafkaConsumer<>(properties);
            kafkaConsumer.subscribe(Collections.singleton(TOPIC_NAME)); // Subscribe consumer to topic(s)
        }

        @Override
        public void run() {
            try {
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
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                kafkaConsumer.close();
                // tell the main code we are done with the consumer
                countDownLatch.countDown();
            }
        }

        public void shutdown() {
            // method to interrupt consumer .poll() and it will throw WakeupException
            kafkaConsumer.wakeup();
        }
    }
}
