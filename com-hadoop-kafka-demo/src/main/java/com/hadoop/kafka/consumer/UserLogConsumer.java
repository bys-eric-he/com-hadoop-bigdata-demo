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
    /**
     * 订阅者
     * 对同一个topic,不同组中的订阅者都会收到消息,即一个topic对应多个consumer,
     * 同一组中的订阅者只有一个consumer能够收到消息,即一个topic对应一个consumer.
     *
     * @param consumerRecord
     */
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
