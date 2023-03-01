# kafka-beginners-course
Hands on for Stephane Maarek's Kafka for beginners course

**Kafka-commands**
```
export TWITTER_CONSUMER_KEY="***"
export TWITTER_CONSUMER_SECRET="***"
export TWITTER_TOKEN="1342492020-***"
export TWITTER_SECRET="***"

export BONSAI_ACCESS_KEY=***
export BONSAI_ACCESS_SECRET=***

tar -xvf kafka_2.13-3.0.0.tgz
pwd = ~/kafka/kafka_2.13-3.0.0
zookeeper-server-start.sh config/zookeeper.properties
kafka-server-start.sh config/server.properties

kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic --create --partitions 3 --replication-factor 1 [--config cleanup.policy=compact --config min.cleanable.dirty.ratio=0.5 --config segment.ms=500]
kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic --describe
kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic second_topic --delete
kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic first_topic
kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic first_topic --producer-property acks=all [--property parse.key=true --property key.separator=,]
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic [--property print.key=true --property key.separator=,]
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic --from-beginning
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic --group first_app
kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --list
kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group second_app
kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --group second_app --topic first_topic --reset-offsets --to-earliest --execute
kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --group second_app --topic first_topic --reset-offsets --shift-by -3 --execute
kafka-configs.sh --bootstrap-server 127.0.0.1:9092 --entity-type topics --entity-name configured-topic --describe
kafka-configs.sh --bootstrap-server 127.0.0.1:9092 --entity-type topics --entity-name configured-topic --add-config min.insync.replicas=2 --alter
kafka-configs.sh --bootstrap-server 127.0.0.1:9092 --entity-type topics --entity-name configured-topic --delete-config min.insync.replicas --alter
```