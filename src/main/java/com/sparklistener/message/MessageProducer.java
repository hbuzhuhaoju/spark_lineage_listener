package com.sparklistener.message;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MessageProducer {

    private static Properties properties;

    static {
        properties = new Properties();
        try (InputStream fis = MessageConsumer.class.getClassLoader().getResourceAsStream("kafka.properties")) {
            properties.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void send(String value) {
        String topic = properties.getProperty("topic");
        send(topic, value);
    }

    public static void send(String topic, String value) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("Sent record to topic %s partition %d offset %d%n",
                    metadata.topic(), metadata.partition(), metadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
