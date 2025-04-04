package com.shangyuchan;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.Random;

/*
 * Simulate out of order (3 sec) and duplicate log producer
 */
public class OutofOrderProducer {
    private static final Logger logger = LoggerFactory.getLogger(Slf4jProducer.class);
    private static final String TOPIC = "test-topic";
    private static final String[] SERVICES = {"AuthService", "PaymentService", "OrderService", "NotificationService"};
    private static final String[] LEVELS = {"TRACE", "DEBUG" ,"INFO", "WARN"};
    private static final Random random = new Random();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            logger.info("OutofOrder producer: Start sending messages");

            for(int i = 0; i < 10; i++) {
                String service = SERVICES[random.nextInt(SERVICES.length)];
                String level = LEVELS[random.nextInt(LEVELS.length)];
                String message = "Log: " + i + " from " + service;

                LogEntry logEntry = new LogEntry(service, level, message);

                // Simulate out-of-order messages: 30% chance, set timestamp 5 seconds earlier.
                if (random.nextDouble() < 0.3) {
                    Instant original = Instant.parse(logEntry.getTimestamp());
                    Instant earlier = original.minus(5, ChronoUnit.SECONDS);
                    logEntry = new LogEntry(logEntry.getId(), earlier.toString(), service, level, message);
                }

                // Send the message.
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, service, logEntry.toString());
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("OutofOrder producer: Error sending record to Kafka", exception);
                    } else {
                        logger.info("Record sent to partition {} with offset {}",
                                    metadata.partition(), metadata.offset());
                    }
                });
                logger.info("Produced log: {}", logEntry);

                // Simulate duplicate messages: 20% chance, send the same log again.
                if (random.nextDouble() < 0.2) {
                    ProducerRecord<String, String> duplicateRecord = new ProducerRecord<>(TOPIC, service, logEntry.toString());
                    producer.send(duplicateRecord, (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("Error sending duplicate record to Kafka", exception);
                        } else {
                            logger.info("Duplicate record sent to partition {} with offset {}",
                                    metadata.partition(), metadata.offset());
                        }
                    });
                    logger.info("Produced duplicate log: {}", logEntry);
                }

                
                Thread.sleep(500);
            }
        } catch (Exception e) {
            logger.error("OutofOrder producer: Unexpected error", e);
        } finally {
            producer.close();
            logger.info("OutofOrder producer: closed");
        }
    }
}
