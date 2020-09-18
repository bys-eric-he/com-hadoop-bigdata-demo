package com.hadoop.kafka.producer;

import com.alibaba.fastjson.JSON;
import com.hadoop.kafka.common.TopicConstant;
import com.hadoop.kafka.handler.KafkaSendResultHandler;
import com.hadoop.kafka.model.UserOrder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class UserOrderProducer {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    private KafkaSendResultHandler producerListener;

    /**
     * 发送消息
     *
     * @param userOrder
     */
    public void sendOrder(UserOrder userOrder) {
        try {
            log.info("---->准备发送用户订单到消息服务：{}", JSON.toJSONString(userOrder));

            //指定在1分区发送内容
            ProducerRecord record = new ProducerRecord(
                    TopicConstant.USER_ORDER_TOPIC_MESSAGE,
                    TopicConstant.USER_ORDER_KEY_2,
                    JSON.toJSONString(userOrder));

            kafkaTemplate.setProducerListener(producerListener);
            kafkaTemplate.send(record);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
