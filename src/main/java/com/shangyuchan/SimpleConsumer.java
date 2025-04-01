package com.shangyuchan;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleConsumer {
    public static void main(String[] args) {
        // Configure the Consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        // group id let kafka knows which consumers to share the same load balancer
        props.put("group.id", "test-group");
        // let kafka to automatically save the consumed offset
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        String topic = "test-topic";
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        try {
            System.out.printf("Start listening to: %s%n", topic);
            while (true) {
                // Poll for new messages
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Received message: key = %s, value = %s, offset = %d%n", 
                                      record.key(), record.value(), record.offset());
                }
            }
        } finally {
            System.out.printf("Consumer closed%n");
            consumer.close();
        }
    }
}
