package com.zouxxyy.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CallBackProducer {

    public static void main(String[] args) {

        // 1 配置文件
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 1); // 重试次数
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // 16k
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1); // 等待时间
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 2 客户端
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 3 发送数据
        for (int i = 0; i < 10; i++) {
            final int finalI = i;
            // 默认按照key的hash值分区
            producer.send(new ProducerRecord<>("first", "key", "zxy--" + i),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e == null) {
                                System.out.println("数据：" + finalI +
                                        " 分区：" + recordMetadata.partition() +
                                        " 偏移量：" + recordMetadata.offset());
                            }
                            else {
                                e.printStackTrace();
                            }
                        }
                    });
        }

        // 4 资源回收
        producer.close();
    }
}
