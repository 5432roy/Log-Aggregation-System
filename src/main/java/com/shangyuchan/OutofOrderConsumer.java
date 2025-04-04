package com.shangyuchan;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutofOrderConsumer {
    private static final Logger logger = LoggerFactory.getLogger(OutofOrderConsumer.class);
    private static final Set<String> processedIds = new HashSet<>();
    private static final List<LogEntry> buffer = new ArrayList<>();
    private static final Map<String, Integer> logCounts = new HashMap<>();

    private static final long WINDOW_DURATION_MS = 3000;

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

        Boolean countPrint = false;

        try {
            logger.info("OutofOrder consumer: starts listening to: {}", topic);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String, String> record : records) {
                    countPrint = true;
                    try {
                        LogEntry logEntry = gson.fromJson(record.value(), LogEntry.class);
                        buffer.add(logEntry);
                    } catch (JsonSyntaxException e) {
                        logger.error("OutofOrder consumer: ", e);
                    }
                }
                processBuffer();

                if (countPrint && buffer.isEmpty()) {
                    logger.info("Aggregated Log Counts: {}", logCounts);
                    countPrint = false;
                }
            }
        } finally {
            consumer.close();
            logger.info("OutofOrder consumer: closed");
        }
    }

    private static void processBuffer() {
        Instant cutoff = Instant.now().minusMillis(WINDOW_DURATION_MS);
        List<LogEntry> readyLogs = new ArrayList<>();

        Iterator<LogEntry> iterator = buffer.iterator();

        while(iterator.hasNext()) {
            LogEntry entry = iterator.next();
            try {
                Instant entryTime = Instant.parse(entry.getTimestamp());
                if (entryTime.isBefore(cutoff)) {
                    readyLogs.add(entry);
                    iterator.remove();
                }
            } catch (DateTimeException e) {
                logger.error("OutofOrder consumer: ", e);
                iterator.remove();
            }
        }

        readyLogs.sort(Comparator.comparing(log -> (Instant.parse(log.getTimestamp()))));

        for (LogEntry logEntry : readyLogs) {
            if (processedIds.contains(logEntry.getId())) {
                logger.info("Duplicate log ID: {}", logEntry.getId());
                continue;
            } else {
                processedIds.add(logEntry.getId());
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
                        logger.error("OutofOrder consumer: Received Log with no LEVEL {}", logEntry);
                        break;
                }
            }
        }
    }
}
