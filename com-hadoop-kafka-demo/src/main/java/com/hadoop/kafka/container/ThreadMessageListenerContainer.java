package com.hadoop.kafka.container;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * 继承消息监听容器
 */
public class ThreadMessageListenerContainer<K, V> extends ConcurrentMessageListenerContainer<K, V> {

    public ThreadMessageListenerContainer(ConsumerFactory<K, V> consumerFactory, ContainerProperties containerProperties) {
        super(consumerFactory, containerProperties);
    }

    public void startContainer() {
        super.doStart();
    }
}