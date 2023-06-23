package org.kafka.course.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class CommonKafkaConfiguration {

    private static CommonKafkaConfiguration instance;

    private String kafkaUri;
    private Logger logger = LoggerFactory.getLogger(CommonKafkaConfiguration.class);

    private CommonKafkaConfiguration() {
        try (InputStream inputStream = CommonKafkaConfiguration.class.getClassLoader().getResourceAsStream("application.properties")) {
            Properties properties = new Properties();
            properties.load(inputStream);
            String host = properties.getProperty("kafka.bootstrap.server.host");
            String port = properties.getProperty("kafka.bootstrap.server.port");
            kafkaUri = host + ":" + port;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            System.exit(2);
        }
    }

    public static CommonKafkaConfiguration getInstance() {
        if (instance == null) {
            instance = new CommonKafkaConfiguration();
        }
        return instance;
    }

    public String getKafkaUriSting() {
        if (kafkaUri == null) {
            throw new RuntimeException("Kafka Uri was not configured");
        }
        return kafkaUri;
    }
}
