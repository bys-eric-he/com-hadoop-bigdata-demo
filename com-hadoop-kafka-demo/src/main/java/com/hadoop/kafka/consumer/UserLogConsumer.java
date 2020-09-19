package com.hadoop.kafka.consumer;

import com.hadoop.kafka.handler.KafkaConsumerResultHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 日志消费者
 */
@Slf4j
@Component
public class UserLogConsumer extends AbstractBaseConsumer {
    @KafkaListener(
            id = "user-log-consumer-one",
            groupId = "kafka_consumer_group_user_log",
            topics = "kafka_topic_user_log_message")
    public void consumer(ConsumerRecord<?, String> consumerRecord) {
        super.action(consumerRecord);
    }

    /**
     * 消费处理
     *
     * @param consumerRecord
     * @return
     */
    @Override
    protected void call(ConsumerRecord<?, String> consumerRecord) {
        KafkaConsumerResultHandler consumerData = new KafkaConsumerResultHandler(consumerRecord);
        try {
            String result = consumerData.call();
            log.info(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
