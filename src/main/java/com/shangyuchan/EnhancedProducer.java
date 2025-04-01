package com.shangyuchan;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;
import java.util.Properties;
import java.util.Random;

/*
 * A producer that simulate a log producer with services type and log levels
 */
public class EnhancedProducer {
    private static final String TOPIC = "test-topic";
    private static final String[] SERVICES = {"AuthService", "PaymentService", "OrderService", "NotificationService"};
    private static final String[] LEVELS = {"INFO", "WARN", "ERROR"};
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
            System.out.printf("Start sending messages%n");
            for(int i = 0; i < 10; i++) {
                String service = SERVICES[random.nextInt(SERVICES.length)];
                String level = LEVELS[random.nextInt(LEVELS.length)];
                String message = "Log: " + i + " from " + service;

                LogEntry logEntry = new LogEntry(service, level, message);
                
                ProducerRecord<String, String> record = new ProducerRecord<String,String>(TOPIC, service, logEntry.toString());
                producer.send(record);

                Thread.sleep(1000);
            }
        } catch(InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
