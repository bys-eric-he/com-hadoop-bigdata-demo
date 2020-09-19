package com.hadoop.kafka.config;

import com.hadoop.kafka.common.TopicConstant;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {

    /**
     * 指定kafka server的地址，集群可配多个，中间，逗号隔开
     */
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * 创建TopicName为TopicConstant.USER_ORDER_TOPIC_MESSAGE的Topic并设置分区数为8以及副本数为1
     * 副本数量不能大于broker 数量，概念问题.
     * 此时直接启动SpringBoot 查询是否有此新建的topic
     *
     * @return
     */
    @Bean
    public NewTopic userOrderTopic() {
        return new NewTopic(TopicConstant.USER_ORDER_TOPIC_MESSAGE, 8, (short) 1);
    }

    /**
     * 创建TopicName为TopicConstant.USER_LOG_TOPIC_MESSAGE的Topic并设置分区数为6以及副本数为1
     * 副本数量不能大于broker 数量，概念问题.
     * 此时直接启动SpringBoot 查询是否有此新建的topic
     *
     * @return
     */
    @Bean
    public NewTopic userLogTopic(){
        return new NewTopic(TopicConstant.USER_LOG_TOPIC_MESSAGE, 6, (short) 1);
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> props = new HashMap<>();
        //配置Kafka实例的连接地址
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(props);
    }

    @Bean
    public AdminClient adminClient() {
        return AdminClient.create(kafkaAdmin().getConfig());
    }
}
