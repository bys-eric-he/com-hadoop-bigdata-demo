package com.hadoop.kafka.handler;

import com.hadoop.kafka.common.SpringContextUtil;
import com.hadoop.kafka.event.publisher.UserOrderPublisher;
import com.hadoop.kafka.model.UserOrder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.concurrent.Callable;

/**
 * 消息消费回调处理
 */
@Slf4j
public class KafkaConsumerResultHandler implements Callable<String> {
    private ConsumerRecord<?, String> record;

    private ObjectMapper objectMapper = new ObjectMapper();

    private UserOrderPublisher userOrderPublisher = (UserOrderPublisher) SpringContextUtil.getBean(UserOrderPublisher.class);

    public KafkaConsumerResultHandler(ConsumerRecord<?, String> record) {
        this.record = record;
    }

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    @Override
    public String call() throws Exception {
        switch (record.topic()) {
            case "kafka_topic_user_order_message": {
                UserOrder userOrder = objectMapper.readValue(record.value(), UserOrder.class);
                log.info("-------->对象转化结果：" + objectMapper.writeValueAsString(userOrder));

                userOrderPublisher.publish(userOrder);
                log.info("--->发布applicationEvent事件:{}", userOrder);
                break;
            }
            case "kafka_topic_user_log_message": {
                break;
            }
        }
        return String.format("--->收到消息：当前线程=%s,主题分区=%s,偏移量=%s,主题=%s,消息内容=%s",
                Thread.currentThread().getName()
                , record.partition()
                , record.offset()
                , record.topic()
                , record.value());
    }
}
