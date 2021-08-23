package com.kafka.thierno;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.producer.ProducerConfig.*;


public class ProducerWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        //properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 1000; i++) {

            //producer record
            String topic = "first-topic";
            String key = String.format("id-%s", i % 100);
            String value = String.format("hello word %s", i);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            logger.info("i: {}, key: {}", i, key);
            //send data
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Error producing data to kafka: ", exception);
                } else {
                    logger.info("Metadata --> \n\ttopic: {}\n\toffset: {}\n\tpartition: {}\n\ttimestamp: {}", metadata.topic(), metadata.offset(), metadata.partition(), metadata.timestamp());
                }
            }).get(); //to block .send(), and make the call synchronous: bad practice
        }

        // flush data
        producer.flush();

        //flush data and close
        producer.close();
    }
}
