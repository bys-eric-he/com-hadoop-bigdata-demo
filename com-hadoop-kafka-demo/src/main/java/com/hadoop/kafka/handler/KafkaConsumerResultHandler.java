package com.hadoop.kafka.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.Callable;

/**
 * 消息消费回调处理
 */
public class KafkaConsumerResultHandler implements Callable<String> {
    private ConsumerRecord<?, String> record;

    public KafkaConsumerResultHandler(ConsumerRecord<?, String> record) {
        this.record = record;
    }

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    @Override
    public String call() throws Exception {
        return String.format("--->收到消息：当前线程=%s,主题分区=%s,偏移量=%s,主题=%s,消息内容=%s",
                Thread.currentThread().getName()
                , record.partition()
                , record.offset()
                , record.topic()
                , record.value());
    }
}
