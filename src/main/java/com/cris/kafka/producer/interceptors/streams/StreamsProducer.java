package com.cris.kafka.producer.interceptors.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * 构建 Kafka Streams
 *
 * @author cris
 * @version 1.0
 **/
public class StreamsProducer {

    public static void main(String[] args) {
        // 原始数据 topic
        String from = "first";
        // 处理后数据存放 topic
        String to = "second";

        Properties prop = new Properties();
        prop.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092");
        prop.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");

        // 流处理配置类
        StreamsConfig config = new StreamsConfig(prop);

        /*构建流处理拓扑关系*/
        TopologyBuilder builder = new TopologyBuilder();
        // 每个步骤都要写它的对接的上一个步骤名！
        builder.addSource("SOURCE", from).addProcessor("PROCESS", () -> new Logfilter(), "SOURCE").addSink("SINK", to, "PROCESS");

        // 创建 Kafka Streams 处理流程对象
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }
}
