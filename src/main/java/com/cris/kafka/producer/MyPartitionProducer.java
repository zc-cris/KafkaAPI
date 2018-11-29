package com.cris.kafka.producer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 新创建 Producer 的 API，使用自定义的分区策略类
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings("Duplicates")
public class MyPartitionProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(String.valueOf(MyCallbackProducer.class));

    public static void main(String[] args) {
        Properties prop = new Properties();

        // Kafka服务端的主机名和端口号
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092");
        // 等待所有副本节点的应答(最严格的数据保存方式，效率也最低，还可以取值 0 或者 1)
        prop.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        // 消息发送失败最大尝试次数
        prop.put(ProducerConfig.RETRIES_CONFIG, 0);
        // 一批消息处理大小
        prop.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 请求延时
        prop.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // 发送缓存区内存大小
        prop.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // key序列化
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        prop.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.cris.kafka.producer.MyPartitioner");

        Producer<String, String> producer = new KafkaProducer<>(prop);
        for (int i = 0; i < 3; i++) {
            producer.send(new ProducerRecord<>("first", "partition" + i));
        }

        producer.close();
    }
}
