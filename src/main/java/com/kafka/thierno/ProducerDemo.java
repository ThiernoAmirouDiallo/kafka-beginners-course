package com.kafka.thierno;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;


public class ProducerDemo {
    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";

        //properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //producer
        KafkaProducer<String, String> stringStringKafkaProducer = new KafkaProducer<>(properties);

        //producer record
        ProducerRecord<String, String> stringProducerRecord = new ProducerRecord<>("first-topic", "hello word");

        //send data
        stringStringKafkaProducer.send(stringProducerRecord);

        // flush data
        stringStringKafkaProducer.flush();

        //flush data and close
        stringStringKafkaProducer.close();
    }
}
