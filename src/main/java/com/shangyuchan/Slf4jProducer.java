package com.shangyuchan;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

public class Slf4jProducer {
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
            logger.info("slf4j consumer: Start sending messages");
            for(int i = 0; i < 10; i++) {
                String service = SERVICES[random.nextInt(SERVICES.length)];
                String level = LEVELS[random.nextInt(LEVELS.length)];
                String message = "Log: " + i + " from " + service;

                LogEntry logEntry = new LogEntry(service, level, message);
                
                ProducerRecord<String, String> record = new ProducerRecord<String,String>(TOPIC, service, logEntry.toString());
                producer.send(record, (metadata, exception) -> {
                    if(exception != null) {
                        logger.error("Error sending record to Kafka", exception);
                    } else {
                        logger.info("Record sent to partition {} with offset {}", 
                            metadata.partition(), metadata.offset());
                    }
                });

                Thread.sleep(500);
            }
        } catch(Exception e) {
            logger.error("slf4j procuder: Unexpected error", e);
        } finally {
            producer.close();
            logger.info("slf4j producer: closed");
        }
    }
}
