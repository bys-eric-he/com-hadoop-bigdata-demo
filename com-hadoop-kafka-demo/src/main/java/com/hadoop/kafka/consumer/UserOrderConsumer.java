package com.hadoop.kafka.consumer;

import com.hadoop.kafka.handler.KafkaConsumerResultHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 用户订单消费者
 *
 * @Configuration
 */
@Slf4j
@Component
public class UserOrderConsumer extends AbstractBaseConsumer {

    /**
     * 配置topic和分区,可以配置多个
     * topic为队列名称
     * partitions表示值的的分区，这里指定了0和1分区
     * partitionOffsets表示详细的指定分区，partition表示那个分区，initialOffset表示Offset的初始位置
     * Consumer Group：我们可以将多个消费组组成一个消费者组，在kafka的设计中同一个分区的数据只能被消费者组中的某一个消费者消费。
     * 同一个消费者组的消费者可以消费同一个topic的不同分区的数据，这也是为了提高kafka的吞吐量！
     *
     * @param consumerRecord
     */
    @KafkaListener(
            id = "consumer-one",
            groupId = "kafka_consumer_group_demo_one",
            topics = "kafka_topic_user_order_message"
    )
    public void consumer_one(ConsumerRecord<?, String> consumerRecord) {
        super.action(consumerRecord);

    }

    /**
     * 配置topic和分区,可以配置多个
     * topic为队列名称
     * partitions表示值的的分区，这里指定了2和3分区
     * partitionOffsets表示详细的指定分区，partition表示那个分区，initialOffset表示Offset的初始位置
     * groupId 不同的消费者消费群组定义另外一个
     *
     * @param consumerRecord
     */
    @KafkaListener(
            id = "consumer-two",
            groupId = "kafka_consumer_group_demo_one",
            topics = "kafka_topic_user_order_message"
    )
    public void consumer_two(ConsumerRecord<?, String> consumerRecord) {
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
