# kafka启动
bin/kafka-server-start.sh -daemon config/server.properties


# topic
bin/kafka-topics.sh --zookeeper localhost:2181 --list

bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic first

bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic first --partitions 3 --replication-factor 1

bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic first


# 生产者
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic first


# 消费者
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first



