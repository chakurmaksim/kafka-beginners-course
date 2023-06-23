package org.kafka.course;

import org.kafka.course.producer.ProducerWithCallBack;
import org.kafka.course.producer.ProducerWithKeys;
import org.kafka.course.producer.SimpleProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

/**
 * Simple Kafka Producers example!
 *
 */
public class ProducerApplication {

    private static Logger logger = LoggerFactory.getLogger(ProducerApplication.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        logger.info("Start Kafka Producers example App.");
        SimpleProducer simpleProducer = new SimpleProducer();
        simpleProducer.send("Hello from Java App :)");
        simpleProducer.close();
        ProducerWithCallBack producerWithCallBack = new ProducerWithCallBack();
        producerWithCallBack.send("Wish you a good day!");
        producerWithCallBack.close();
        ProducerWithKeys producerWithKeys = new ProducerWithKeys();
        producerWithKeys.send("Message with a Key:");
        logger.info("Stop Kafka Producers example App.");
    }
}
