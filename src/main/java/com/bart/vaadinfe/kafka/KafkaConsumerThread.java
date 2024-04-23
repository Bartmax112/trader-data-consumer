package com.bart.vaadinfe.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerThread extends Thread{

    KafkaConsumer<String, String> consumer;
    String topic;

    public KafkaConsumerThread(String topic) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","kafka-1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());

        properties.setProperty("value.deserializer",StringDeserializer.class.getName());
        properties.setProperty("auto.offset.reset","earliest");
        properties.setProperty("group.id","my-kafka-group");
        this.topic = topic;
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(topic));
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(topic));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Received message: " + record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

}
