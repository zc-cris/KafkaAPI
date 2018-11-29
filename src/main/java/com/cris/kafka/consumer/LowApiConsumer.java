package com.cris.kafka.consumer;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class LowApiConsumer {
    @SuppressWarnings("all")
    public static void main(String[] args) throws Exception {

        BrokerEndPoint leader = null;

        // 创建简单消费者
        String host = "hadoop101";
        int port = 9092;

        // 获取分区的leader
        SimpleConsumer metaConsumer = new SimpleConsumer(host, port, 500, 10 * 1024, "metadata");

        // 获取元数据信息
        TopicMetadataRequest request = new TopicMetadataRequest(Arrays.asList("first"));
        TopicMetadataResponse response = metaConsumer.send(request);

        leaderLabel:
        for (TopicMetadata topicMetadata : response.topicsMetadata()) {
            if ("first".equals(topicMetadata.topic())) {
                // 关心的主题元数据信息
                for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
                    int partid = partitionMetadata.partitionId();
                    if (partid == 1) {
                        // 关心的分区元数据信息
                        leader = partitionMetadata.leader();
                        break leaderLabel;
                    }
                }
            }
        }

        if (leader == null) {
            System.out.println("分区信息不正确");
            return;
        }

        // host,port应该是指定分区的leader
        SimpleConsumer consumer = new SimpleConsumer(leader.host(), leader.port(), 500, 10 * 1024, "accessLeader");

        // 抓取数据
        FetchRequest req = new FetchRequestBuilder().addFetch("first", 1, 5, 10 * 1024).build();
        FetchResponse resp = consumer.fetch(req);

        ByteBufferMessageSet messageSet = resp.messageSet("first", 1);

        for (MessageAndOffset messageAndOffset : messageSet) {
            ByteBuffer buffer = messageAndOffset.message().payload();
            byte[] bs = new byte[buffer.limit()];
            buffer.get(bs);
            String value = new String(bs, "UTF-8");
            System.out.println(value);
        }
    }
}
