package com.zouxxyy.producer;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class MyProducer {

    public static void main(String[] args) {

        Properties properties = new Properties();

        new KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    }
}
