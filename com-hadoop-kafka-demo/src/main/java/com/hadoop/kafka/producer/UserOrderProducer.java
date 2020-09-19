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

            // 使用OrderID哈希值对主题分区数量取模,让数据发送到不同的分区,达到负载均衡，提高kafka的吞吐量.
            String key = userOrder.getOrderID();
            int hCode = key.hashCode();
            // 这里是分8个分片；需要注意的是hashcode有可能为负数；可以通过&操作；
            // 或者直接Math.Abs(hCode%2)也可以
            int partitionNum = (hCode & 0x7fffffff) % 8;

            // 发送内容
            ProducerRecord<?, String> record = new ProducerRecord<>(
                    TopicConstant.USER_ORDER_TOPIC_MESSAGE,
                    partitionNum,
                    JSON.toJSONString(userOrder));

            kafkaTemplate.setProducerListener(producerListener);
            kafkaTemplate.send(record);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
