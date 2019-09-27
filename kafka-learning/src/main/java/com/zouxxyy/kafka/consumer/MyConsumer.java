package com.zouxxyy.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {

    public static void main(String[] args) {

        // 1 配置文件
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); // 开启自动提交
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000); // 提交offset时间间隔
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "zxyGroup1"); // 消费者组
        // 注意只有新组或者offset丢失时，才会初始化offset的值，它默认是最新的。因此新加的组默认都是从最新的消费位置开始消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 这里手动设置从最旧的数据开始消费

        // 2 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 3 订阅主题
        consumer.subscribe(Arrays.asList("first", "Second"));

        while (true){
            // 4 获取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100); // 批量获取

            // 5 打印数据
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(" 分区：" + consumerRecord.partition() +
                        " 偏移量：" + consumerRecord.offset() +
                        " key： " + consumerRecord.key() +
                        " value：" + consumerRecord.value());
            }
        }
    }
}



/*
一个有趣的地方，我关consumer时不小心点成了Disconnect，导致项目一直在后台跑
我没发现这个问题，结果测试时consumer打印时总是少了个分区。。。而且关闭consumer后，producer生成的数据也总是丢
后面检查jps发现，原来被这个后台的consumer偷偷吃了。。
 */