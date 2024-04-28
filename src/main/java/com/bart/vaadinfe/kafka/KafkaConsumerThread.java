package com.bart.vaadinfe.kafka;

import com.vaadin.flow.component.Component;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerThread extends Thread{

    KafkaConsumer<String, DataStructure> consumer;
    String topic;
    Component chart;

    public KafkaConsumerThread(String topic, Component chart) {

        this.chart = chart;
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
                ConsumerRecords<String, DataStructure> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, DataStructure> record : records) {
                    System.out.println("Received message: \n" + record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

}
