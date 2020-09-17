package com.hadoop.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * 日志消费者
 */
@Slf4j
@Component
public class UserLogConsumer {
    @KafkaListener(topics = "kafka_topic_user_log_message")
    public void consumer(ConsumerRecord consumerRecord) {
        Optional<Object> optionalMessage = Optional.ofNullable(consumerRecord.value());
        Optional<String> optionalTopic = Optional.ofNullable(consumerRecord.topic());
        String topic = null;
        Object message = null;

        if (optionalTopic.isPresent()) {
            topic = optionalTopic.get();
        }

        if (optionalMessage.isPresent()) {
            message = optionalMessage.get();
        }

        log.info("-->收到消息 >主题:{},>内容:{}", topic, message);
    }
}
