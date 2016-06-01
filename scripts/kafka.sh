#!/bin/bash

export KAFKA_HOME=/usr/hdp/2.4.2.0-258/kafka
export KAFKA_BROKER_ADDRESS=sandbox.hortonworks.com:6667
export ZOOKEEPER_ADDRESS=localhost:2181
export TOPIC_NAME="test2"
export FILE_PATH=/media/sf_shared_folder/stream.20130606-ab.txt

#echo "Creating topic '$TOPIC_NAME'"
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER_ADDRESS --replication-factor 1 --partition 1 --topic $TOPIC_NAME

$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list $KAFKA_BROKER_ADDRESS --topic test < $FILE_PATH