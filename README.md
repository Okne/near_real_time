# near_real_time

## How to run
* import project into you IDE
* go to scripts folder
* connect to your cassandra cluster and creat keyspace and tables with 'cassandra_scripts.cql' file
* update kafka.sh with you env properties and run script to create kafka topic and fulfill it with messages from bidding logs
* update es_index_creations.sh with you env properties and run to create index with custom mappings
* run main class from App object with the following params: %app_name% %master% %kafka_topic_name% %zookeeper_address%
