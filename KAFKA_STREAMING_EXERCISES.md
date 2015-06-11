#Spark Streaming Exercises

Kafka Samples
==========

Start Kafka, create the topics and test:

bin/zookeeper-server-start.sh config/zookeeper.properties 

bin/kafka-server-start.sh config/server.properties 

bin/kafka-create-topic.sh --zookeeper localhost:2181 --replica 1 --partition 1 --topic wordbucket

bin/kafka-list-topic.sh --zookeeper localhost:2181

bin/kafka-console-producer.sh --broker-list localhost:9092 --topic wordbucket

bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic wordbucket --from-beginning

Setup and Run DSE
=============================
[If running DSE 4.6 fix DSE so Kafka works](https://support.datastax.com/hc/en-us/articles/204226489--java-lang-NoSuchMethodException-seen-when-attempting-Spark-streaming-from-Kafka)

Run DSE as an analytics node:
dse/bin/dse cassandra -k

To build and run the Kafka Feeder Example
========================================

* cd into the KafkaFeeder directory and build and run the project in that directory
* Build the jar file -> 'mvn package'
* java -jar target/KafkaFeeder-0.1-jar-with-dependencies.jar localhost:9092
* Make sure you've got a running spark server and Cassandra node listening on localhost
* Make sure you've got a running Kafka server on localhost with the topic events pre-provisioned.

To build and run the Kafka Streaming
=========================================

~/dse/bin/dse spark-submit --class kafkaStreaming.RunKafkaFeeder ./target/AdvanceSpark-0.1-jar-with-dependencies.jar