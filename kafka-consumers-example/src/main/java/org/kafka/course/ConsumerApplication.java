package org.kafka.course;

import org.kafka.course.consumer.ConsumerWithThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple Kafka Consumers example!
 *
 */
public class ConsumerApplication {

    private static Logger logger = LoggerFactory.getLogger(ConsumerApplication.class);

    public static void main( String[] args ) {
        logger.info("Start Kafka Consumers example App.");
        ConsumerWithThread consumerWithThread = new ConsumerWithThread();
        consumerWithThread.startListening();
        logger.info("Stop Kafka Consumers example App.");
    }
}
