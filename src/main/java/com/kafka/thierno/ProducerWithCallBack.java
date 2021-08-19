package com.kafka.thierno;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;


public class ProducerWithCallBack {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerWithCallBack.class);

        String bootstrapServers = "127.0.0.1:9092";

        //properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 200; i++) {

            //producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first-topic", String.format("hello word %s", i));

            //send data
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Error producing data to kafka: ", exception);
                } else {
                    logger.info("Metadata --> \n\ttopic: {}\n\toffset: {}\n\tpartition: {}\n\ttimestamp: {}", metadata.topic(), metadata.offset(), metadata.partition(), metadata.timestamp());
                }
            });
        }

        // flush data
        producer.flush();

        //flush data and close
        producer.close();
    }
}
