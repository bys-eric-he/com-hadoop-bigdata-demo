package com.hadoop.kafka.producer;

import com.alibaba.fastjson.JSON;
import com.hadoop.kafka.common.TopicConstant;
import com.hadoop.kafka.handler.KafkaSendResultHandler;
import com.hadoop.kafka.model.UserLog;
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
     * @param userLog
     */
    public void sendlog(UserLog userLog) {
        try {
            log.info("---->准备发送用户日志到消息服务：{}", JSON.toJSONString(userLog));

            // 指定key让kafka自动判断partition
            String key = userLog.getUserId();
            ProducerRecord<?, String> record = new ProducerRecord<>(
                    TopicConstant.USER_LOG_TOPIC_MESSAGE,
                    key,
                    JSON.toJSONString(userLog));

            kafkaTemplate.setProducerListener(producerListener);
            kafkaTemplate.send(record);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}