package com.hadoop.kafka.handler;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

/**
 * 消息结果回调
 */
@Component
public class KafkaSendResultHandler implements ProducerListener<String, ProducerRecord<?, String>> {

    private static final Logger log = LoggerFactory.getLogger(KafkaSendResultHandler.class);

    /**
     * 消息发送成功回调
     *
     * @param producerRecord
     * @param recordMetadata
     */
    @Override
    public void onSuccess(ProducerRecord producerRecord, RecordMetadata recordMetadata) {
        log.info("---->消息发送成功回调 : " + producerRecord.toString());
    }

    /**
     * 消息发送失败回调
     *
     * @param producerRecord
     * @param exception
     */
    @Override
    public void onError(ProducerRecord producerRecord, Exception exception) {
        log.info("---->消息发送失败回调 : " + producerRecord.toString());
    }

    /**
     * 是否开启发送监听
     *
     * @return true开启，false关闭
     */
    @Override
    public boolean isInterestedInSuccess() {
        return true;
    }
}