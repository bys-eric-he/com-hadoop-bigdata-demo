package com.hadoop.kafka.listener;

import com.hadoop.kafka.handler.KafkaConsumerResultHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

/**
 * 默认生产者监听器
 */
@Slf4j
@Component
public class ConsumerListener implements MessageListener<String, String> {

    /**
     * 消费消息
     *
     * @param consumerRecord 消息
     */
    @Override
    public void onMessage(ConsumerRecord<String, String> consumerRecord) {
        KafkaConsumerResultHandler consumerData = new KafkaConsumerResultHandler(consumerRecord);
        try {
            log.info("---------默认生产者监听器收到消息-----------");
            String result = consumerData.call();
            log.info(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}