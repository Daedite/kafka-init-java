package org.example.producer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.util.Properties;

public class ProducerMessage {
    public static void produce() {
        Properties kafkaProps = new Properties();

        try {
            URL resource = ProducerMessage.class.getClassLoader().getResource("producer.properties");
            if (resource == null) {
                throw new IllegalArgumentException("file not found!");
            }
            kafkaProps.load(new FileReader(resource.getFile()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Producer<String, String> producer = new KafkaProducer<>(kafkaProps);
        String recordValue = "Current time is " + Instant.now().toString();

        ProducerRecord<String, String> record = new ProducerRecord<>("test", null, recordValue);

        producer.send(record);
        producer.flush();
    }

    public static void main(String[] args) {
        produce();
    }
}