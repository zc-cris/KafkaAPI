package com.cris.kafka.consumer;


import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * TODO
 *
 * @author cris
 * @version 1.0
 **/
public class SimpleAPIConsumer {

    private List<String> replicate_brokers = null;

    SimpleAPIConsumer() {
        replicate_brokers = new ArrayList<>();
    }

    public static void main(String[] args) {
        SimpleAPIConsumer simpleAPIConsumer = new SimpleAPIConsumer();

        // 最大读取消息数量
        long maxReads = Long.parseLong("1");
        // 要订阅的topic
        String topic = "first";
        // 要查找的分区
        int partition = Integer.parseInt("1");

        // broker节点的ip
        List<String> seeds = new ArrayList<>();
        seeds.add("hadoop101");
        seeds.add("hadoop102");
        seeds.add("hadoop103");

        // 端口
        String port = "9092";

        try {
            simpleAPIConsumer.run(maxReads, topic, partition, seeds, port);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

    }

    private void run(long maxReads, String topic, int partition, List<String> seeds, String port) throws UnsupportedEncodingException {

        // 获取到指定主题的指定分区的分区元数据
        PartitionMetadata partitionMetadata = findLeader(seeds, port, topic, partition);
        if (partitionMetadata == null) {
            throw new RuntimeException("no such partition!");
        }
        BrokerEndPoint leader = partitionMetadata.leader();
        if (leader == null) {
            throw new RuntimeException("no such topic!");
        }
        String clientName = "get_appointed_Message";

        SimpleConsumer simpleConsumer = new SimpleConsumer(leader.host(), Integer.parseInt(port), 1000, 64 * 1024, clientName);
        // 获取上一次读取的偏移量信息
        long readOffset = getLastOffset(simpleConsumer, topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
        while (maxReads > 0) {
            kafka.api.FetchRequest fetchRequest = new FetchRequestBuilder().clientId(clientName).addFetch(topic, partition, readOffset, 10 * 1024).build();
            FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);
            ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);
            for (MessageAndOffset messageAndOffset : messageAndOffsets) {
                ByteBuffer byteBuffer = messageAndOffset.message().payload();
                byte[] bytes = new byte[byteBuffer.limit()];
                byteBuffer.get(bytes);
                System.out.println("new String(bytes,\"UTF-8\") = " + new String(bytes, "UTF-8"));
                readOffset = messageAndOffset.nextOffset();
            }
            maxReads--;
        }

    }

    private long getLastOffset(SimpleConsumer simpleConsumer, String topic, int partition, long earliestTime, String clientName) {

        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfoMap =
                new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();

        requestInfoMap.put(topicAndPartition, new PartitionOffsetRequestInfo(earliestTime, 1));
        OffsetRequest offsetRequest = new OffsetRequest(requestInfoMap, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse offsetResponse = simpleConsumer.getOffsetsBefore(offsetRequest);

        if (offsetResponse.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + offsetResponse.errorCode(topic, partition));
            return 0;
        }

        return offsetResponse.offsets(topic, partition)[0];
    }


    private PartitionMetadata findLeader(List<String> seeds, String port, String topic, int partition) {
        PartitionMetadata result = null;
        loop:
        for (String seed : seeds) {
            SimpleConsumer simpleConsumer = new SimpleConsumer(seed, Integer.parseInt(port), 10000, 64 * 1024, "leaderLookup");
            // 这里发送对指定主题的元数据请求信息，值得一提的是，主题参数为集合类型
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Arrays.asList(topic));
            // 发送请求并得到对应的元数据信息
            TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);
            // 遍历元数据，因为默认获取的是不同主题的元数据信息
            List<TopicMetadata> topicMetadatas = topicMetadataResponse.topicsMetadata();
            for (TopicMetadata topicMetadata : topicMetadatas) {
                // 根据 topic 元数据得到分区元数据
                List<PartitionMetadata> partitionMetadatas = topicMetadata.partitionsMetadata();
                // 遍历分区元数据得到和指定分区号匹配的分区元数据信息
                for (PartitionMetadata partitionMetadata : partitionMetadatas) {
                    if (partitionMetadata.partitionId() == partition) {
                        result = partitionMetadata;
                        // 跳出外层循环
                        break loop;
                    }
                }
            }
        }
        return result;
    }
}
