package com.hadoop.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * 用户订单消费者
 *
 * @Configuration
 */
@Slf4j
@Component
public class UserOrderConsumer {

    /**
     * 配置topic和分区,可以配置多个
     * topic为队列名称
     * partitions表示值的的分区，这里指定了0和1分区
     * partitionOffsets表示详细的指定分区，partition表示那个分区，initialOffset表示Offset的初始位置
     *
     * @param consumerRecord
     */
    @KafkaListener(id = "KafkaListener-one", groupId = "kafka_consumer_group_demo_one", topicPartitions =
            {@TopicPartition(topic = "kafka_topic_user_order_message",
                    partitions = {"0", "2"},
                    partitionOffsets = @PartitionOffset(partition = "1", initialOffset = "4"))
            }
    )
    public void consumer_one(ConsumerRecord consumerRecord) {
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

        log.info("--> Consumer-One 收到消息 >主题:{},>内容:{}", topic, message);

    }

    /**
     * 配置topic和分区,可以配置多个
     * topic为队列名称
     * partitions表示值的的分区，这里指定了2和3分区
     * partitionOffsets表示详细的指定分区，partition表示那个分区，initialOffset表示Offset的初始位置
     * groupId 不同的消费者消费群组定义另外一个
     * @param consumerRecord
     */
    @KafkaListener(id = "KafkaListener-two", groupId = "kafka_consumer_group_demo_two", topicPartitions =
            {@TopicPartition(topic = "kafka_topic_user_order_message",
                    partitions = {"2", "3"},
                    partitionOffsets = @PartitionOffset(partition = "1", initialOffset = "4"))
            }
    )
    public void consumer_two(ConsumerRecord consumerRecord) {
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

        log.info("--> Consumer-Two 收到消息 >主题:{},>内容:{}", topic, message);

    }
}
