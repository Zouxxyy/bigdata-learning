package com.zouxxyy.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

public class InterceptorProducer {

    public static void main(String[] args) throws InterruptedException {
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
        // 添加拦截器
        ArrayList<String> interceptors = new ArrayList<>();
        interceptors.add("com.zouxxyy.kafka.interceptor.TimeInterceptor");
        interceptors.add("com.zouxxyy.kafka.interceptor.CounterInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        // 2 客户端
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 3 发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first", "zxy--" + i));
            // 可以在后面调用get()方法，实现同步发送
        }

        Thread.sleep(5000);
        System.out.println("*************");

        // 4 资源回收(也会执行拦截器的close方法)
        producer.close();
    }
}
