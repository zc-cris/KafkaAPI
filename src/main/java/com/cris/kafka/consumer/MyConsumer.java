package com.cris.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

/**
 * 自定义消费者
 *
 * @author cris
 * @version 1.0
 **/
public class MyConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(String.valueOf(MyConsumer.class));

    public static void main(String[] args) {

        Properties prop = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上，这里消费者需要去连接 broker
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092");
        // 指定consumer group
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        // 是否自动确认offset
        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 自动确认offset的时间间隔
        prop.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // key的反序列化类
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // value的反序列化类
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 定义consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(prop);
        // 消费者订阅的topic, 可同时订阅多个
        consumer.subscribe(Arrays.asList("first"));

        while (true) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, partition = %d, key = %s, value = %s%n", record.offset(), record.partition(), record.key(), record.value());
            }
        }
    }
}
