package com.hadoop.kafka.factory;

import com.hadoop.kafka.container.ThreadMessageListenerContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * 消费者工厂
 */
@Slf4j
@Configuration("customerKafkaConsumerFactory")
public class KafkaConsumerFactory extends AbstractBaseFactory {
    /**
     * 获取消费者工厂
     */
    public ConsumerFactory<String, String> consumerFactory() {

        // 消费者配置信息
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBootstrapServersConfig());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, this.getEnableAutoCommitConfig());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, this.getMaxPollRecordsConfig());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * 容器配置
     *
     * @param clientId  客户端ID
     * @param groupId   组名
     * @param clazz     消费者监听器
     * @param topicName topicName
     * @return 容器配置
     */
    public ContainerProperties containerProperties(String clientId, String groupId, Class clazz, String topicName) {

        ContainerProperties containerProperties = new ContainerProperties(topicName);
        containerProperties.setAckMode(ContainerProperties.AckMode.RECORD);
        containerProperties.setGroupId(groupId);
        containerProperties.setClientId(clientId);
        containerProperties.setMessageListener(this.getContext().getBean(clazz));
        return containerProperties;
    }

    /**
     * 获取消费容器实例
     *
     * @param clientId    客户端ID
     * @param groupId     组名
     * @param clazz       消费者监听器
     * @param topicName   topicName
     * @param threadCount 消费线程数
     * @return 消息监听容器
     */
    public ThreadMessageListenerContainer<String, String> kafkaListenerContainer(String clientId, String groupId, Class clazz, String topicName, int threadCount) {

        ThreadMessageListenerContainer<String, String> container
                = new ThreadMessageListenerContainer<>(
                consumerFactory(), containerProperties(clientId, groupId, clazz, topicName));
        container.setConcurrency(threadCount);
        container.getContainerProperties().setPollTimeout(3000);
        return container;
    }
}
