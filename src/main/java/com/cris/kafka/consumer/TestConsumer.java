package com.cris.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class TestConsumer {
    public static void main(String[] args) {

        // 创建配置对象
        Properties prop = new Properties();

        prop.setProperty("bootstrap.servers", "hadoop101:9092");

        prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        prop.setProperty("group.id", "cris");
        prop.setProperty("enable.auto.commit", "true");
        prop.setProperty("auto.commit.interval.ms", "1000");

        // 创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

        // 订阅主题
        consumer.subscribe(Arrays.asList("first"));

        while (true) {

            // 拉取数据
            ConsumerRecords<String, String> records = consumer.poll(500);

            for (ConsumerRecord<String, String> record : records) {
                // 打印数据
                System.out.println(record);
            }
        }

    }
}
