package com.hadoop.kafka.producer;

import com.alibaba.fastjson.JSON;
import com.hadoop.kafka.common.TopicConstant;
import com.hadoop.kafka.handler.KafkaSendResultHandler;
import com.hadoop.kafka.model.Userlog;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * 用户日志生产者
 */
@Slf4j
@Component
public class UserLogProducer {
    @Autowired
    private KafkaTemplate kafkaTemplate;
    @Autowired
    private KafkaSendResultHandler producerListener;

    /**
     * 发送消息
     *
     * @param userlog
     */
    public void sendlog(Userlog userlog) {
        try {
            log.info("---->准备发送用户日志到消息服务：{}", JSON.toJSONString(userlog));
            ProducerRecord record = new ProducerRecord(TopicConstant.USER_LOG_MESSAGE, JSON.toJSONString(userlog));
            kafkaTemplate.setProducerListener(producerListener);
            kafkaTemplate.send(record);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}