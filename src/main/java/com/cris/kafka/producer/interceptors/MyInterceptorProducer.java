package com.cris.kafka.producer.interceptors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

/**
 * 构建拦截器链对 Producer 发送的消息做处理
 *
 * @author cris
 * @version 1.0
 **/
public class MyInterceptorProducer {

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

        /*构建拦截器链*/
        prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                Arrays.asList("com.cris.kafka.producer.interceptors.TimeProducerInterceptor", "com.cris.kafka.producer.interceptors.CountProducerInterceptor"));

        Producer<String, String> producer = new KafkaProducer<>(prop);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first", "interceptor" + i));
        }

        /*一定要关闭Producer，这样才会调用拦截器的 close 方法*/
        producer.close();
    }
}
