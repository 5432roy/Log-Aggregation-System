package com.shangyuchan;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Slf4jConsumer {
    private static final Logger logger = LoggerFactory.getLogger(Slf4jConsumer.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String topic = "test-topic";
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        Gson gson = new Gson();

        Map<String, Integer> logCounts = new HashMap<>();

        try {
            logger.info("slf4j consumer: starts listening to: {}", topic);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        LogEntry logEntry = gson.fromJson(record.value(), LogEntry.class);
                        logCounts.put(logEntry.getLevel(), logCounts.getOrDefault(logEntry.getLevel(), 0) + 1);
                        switch (logEntry.getLevel()) {
                            case "TRACE":
                                logger.trace("{}", logEntry);
                                break;
                            case "DEBUG":
                                logger.debug("{}", logEntry);
                                break;
                            case "INFO":
                                logger.info("{}", logEntry);
                                break;
                            case "WARN":
                                logger.warn("{}", logEntry);
                                break;
                            default:
                                logger.error("Received Log with no LEVEL {}", logEntry);
                                break;
                        }
                    } catch (JsonSyntaxException e) {
                        logger.error("slf4j consumer: Failed to parse log message: {}", record.value() ,e);
                    }
                }

                if (!records.isEmpty()) {
                    logger.info("Aggregated Log Counts: {}", logCounts);
                }
            }
        } catch (Exception e) {
        } finally {
            consumer.close();
            logger.info("slf4j consumer: closed");
        }
    }
}
