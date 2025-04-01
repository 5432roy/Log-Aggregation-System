package com.shangyuchan;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;
import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {
        // Configure the Producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        // Successfully send to message when a replicas sync from the leader
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            // Send a series of messages to the topic "test-topic"
            System.out.printf("Start sending messages%n");
            for (int i = 0; i < 10; i++) {
                String key = "Key" + i;
                String value = "Hello Kafka " + i;
                ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", key, value);
                producer.send(record);
                System.out.println("Sent message: " + record.toString());
            }
        } finally {
            producer.close();
        }
    }
}