package com.cris.kafka.producer.interceptors.streams;


import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.io.UnsupportedEncodingException;

/**
 * 具体的数据清洗类
 *
 * @author cris
 * @version 1.0
 **/
public class Logfilter implements Processor<byte[], byte[]> {

    private ProcessorContext processorContext;

    @Override
    public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;
    }

    @Override
    public void process(byte[] key, byte[] value) {
        try {
            /*注意：这里我们可以针对 key 和 value 进行字节程度上的处理*/
            String string = new String(value, "UTF-8");
            // 如果包含“>>>”则只保留该标记后面的内容
            if (string.contains(">>>")) {
                string = string.split(">>>")[1].trim();
            }
            // 处理后继续输出到下一个 topic
            this.processorContext.forward(key, string.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
