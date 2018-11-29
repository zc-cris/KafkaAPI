package com.cris.kafka.producer.interceptors;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义对生产的消息做出改变（在 value 前面加上时间戳）
 *
 * @author cris
 * @version 1.0
 **/
public class TimeProducerInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        /*需要注意的是：这里要新建一个 ProducerRecord 存放新的 value，并且 topic 最好保持一致*/
        String value = producerRecord.value();
        value = System.currentTimeMillis() + value;
        ProducerRecord<String, String> record = new ProducerRecord<>(producerRecord.topic(), value);
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
