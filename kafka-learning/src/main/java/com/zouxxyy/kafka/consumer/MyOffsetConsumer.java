package com.zouxxyy.kafka.consumer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * 自定义offset
 * 这节很值得揣摩，通过它可以了解消费者的具体行为，比如消费者在Rebalance时得到自己要处理的分区以及offset
 */

public class MyOffsetConsumer {

    // 这里的offset，key由(topic, partition) 决定，我觉得应该还要加上组名
    private static Map<TopicPartition, Long> currentOffset = new HashMap<>();

    public static void main(String[] args) {

        // 1 配置文件
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // 关闭自动提交
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "zxyGroup1"); // 消费者组

        // 2 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 3 订阅主题
        consumer.subscribe(Arrays.asList("first", "second"), new ConsumerRebalanceListener() {

            // Rebalance前, 提交offset
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                commitOffset(currentOffset);
            }

            // Rebalance后，重新分配offset
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                currentOffset.clear();
                for (TopicPartition partition : partitions) {
                    consumer.seek(partition, getOffset(partition));// 定位到最近提交的offset的位置，继续消费
                }
            }
        });

        while (true) {
            // 4 获取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100); // 批量获取

            // 5 打印数据
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(" 分区：" + consumerRecord.partition() +
                        " 偏移量：" + consumerRecord.offset() +
                        " key： " + consumerRecord.key() +
                        " value：" + consumerRecord.value());
                currentOffset.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), consumerRecord.offset());
                System.out.println(currentOffset.toString());
            }

            // 6 提交offset
            commitOffset(currentOffset); //自定义的提交offset方法
        }
    }

    // 自定义获取 offset
    private static long getOffset(TopicPartition partition) {
        // 可以发现所有分区都从10开始，表名我们在启动新节点时，会进行一次Rebalance
        return 10;
    }

    // 自定义提交 offset
    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {
        // 将偏移量提交到非易失环境中
    }
}
