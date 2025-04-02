package com.shangyuchan;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import com.google.gson.Gson;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class EnhancedConsumer {
    
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
            System.out.println("Start listening to: " + topic);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis((100)));
                for (ConsumerRecord<String, String> record : records) {
                    LogEntry logEntry = gson.fromJson(record.value(), LogEntry.class);
                    
                    logCounts.put(logEntry.getLevel(), logCounts.getOrDefault(logEntry.getLevel(), 0) + 1);
                    
                    System.out.println(logEntry);
                }

                if (!records.isEmpty()) {
                    System.out.println("Log Counts:" + logCounts);
                }
            }
        } finally {
            consumer.close();
        }
    }
}
