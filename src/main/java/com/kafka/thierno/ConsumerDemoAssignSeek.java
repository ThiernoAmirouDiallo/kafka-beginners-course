package com.kafka.thierno;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;


public class ConsumerDemoAssignSeek {
    static Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first-topic";

        //properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //assign
        TopicPartition partition = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partition));

        //seek
        long offsetToReadFrom = 15L;
        consumer.seek(partition, offsetToReadFrom);

        //poll new data
        int noMessagesToRead = 5;
        int readMessages = 0;
        while (readMessages < noMessagesToRead) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key --> {} \n\tValue: {}\n\ttopic: {}\n\toffset: {}\n\tpartition: {}\n\ttimestamp: {}", record.key(), record.value(), record.topic(), record.offset(), record.partition(), record.timestamp());

                readMessages++;
                if (readMessages >= noMessagesToRead) {
                    break;
                }
            }
        }

        logger.info("exiting the application");
    }
}
