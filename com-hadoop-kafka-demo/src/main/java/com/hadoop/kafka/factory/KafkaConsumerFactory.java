package com.hadoop.kafka.factory;

import com.hadoop.kafka.container.ThreadMessageListenerContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Locale;
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
        //GroupID
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.getGroupID());
        //连接地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBootstrapServersConfig());
        //是否自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, this.getEnableAutoCommitConfig());
        //控制单次调用call方法能够返回的记录数量，帮助控制在轮询里需要处理的数据量
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, this.getMaxPollRecordsConfig());
        //Session超时设置
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, this.getSessionTimeOut());
        //consumer 读取级别 （开启事务的时候一定要设置为读已提交）
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));
        //键的反序列化方式
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //值的反序列化方式
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
