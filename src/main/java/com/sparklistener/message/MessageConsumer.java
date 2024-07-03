package com.sparklistener.message;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MessageConsumer {
    public static void main(String[] args) {
        // 读取配置文件
        Properties properties = new Properties();

        try (InputStream fis = MessageConsumer.class.getClassLoader().getResourceAsStream("kafka.properties")) {
            properties.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        String topic = properties.getProperty("topic");
        consumer.subscribe(Collections.singletonList(topic));

        // 消费消息
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", 
                                      record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
