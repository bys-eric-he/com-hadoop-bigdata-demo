package com.hadoop.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public abstract class AbstractBaseConsumer {
    /**
     * 消费处理
     *
     * @param consumerRecord
     * @return
     */
    protected abstract void call(ConsumerRecord<?, String> consumerRecord);

    public void action(ConsumerRecord<?, String> consumerRecord) {
        this.call(consumerRecord);
    }
}
