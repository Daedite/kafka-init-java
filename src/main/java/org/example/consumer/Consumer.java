package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.producer.ProducerMessage;

import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public void consume(){
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
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        kafkaConsumer.subscribe(Collections.singleton("test"));
        while (true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records){
                System.out.println("MESSAGE RECEIVED: "+ record);
                System.out.println("MESSAGE RECEIVED: "+ record.value());
            }
            kafkaConsumer.commitAsync();
        }
    }

    public static void main(String[] args) {
        new Consumer().consume();
    }
}
