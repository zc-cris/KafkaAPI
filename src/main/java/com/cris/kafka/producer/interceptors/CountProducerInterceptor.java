package com.cris.kafka.producer.interceptors;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 注意拦截器是单例的，这里统计消息发送成功和失败的个数
 *
 * @author cris
 * @version 1.0
 **/
public class CountProducerInterceptor implements ProducerInterceptor<String, String> {

    private int successCount;
    private int failedCount;


    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {

        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        /*消息发送成功+1*/
        if (e == null) {
            ++successCount;
        } else {
            /*消息发送失败+1*/
            ++failedCount;
        }
    }

    @Override
    public void close() {
        // 保存结果
        System.out.println("Successful sent: " + successCount);
        System.out.println("Failed sent: " + failedCount);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
