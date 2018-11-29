package com.cris.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 自定义分区策略类
 *
 * @author cris
 * @version 1.0
 **/
public class MyPartitioner implements Partitioner {

    private static final Logger LOGGER = LoggerFactory.getLogger(String.valueOf(MyCallbackProducer.class));

    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        // 控制分区，始终将消息发送到 1 号分区
        return 1;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
