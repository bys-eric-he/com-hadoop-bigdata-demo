package com.hadoop.kafka.config;

import com.hadoop.kafka.common.TopicConstant;
import com.hadoop.kafka.factory.KafkaConsumerFactory;
import com.hadoop.kafka.factory.KafkaProducerFactory;
import com.hadoop.kafka.handler.KafkaSendResultHandler;
import com.hadoop.kafka.listener.ConsumerListener;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    /**
     * 指定kafka server的地址，集群可配多个，中间，逗号隔开
     */
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Autowired
    @Qualifier("customerKafkaProducerFactory")
    private KafkaProducerFactory customerKafkaProducerFactory;

    @Autowired
    @Qualifier("customerKafkaConsumerFactory")
    private KafkaConsumerFactory customerKafkaConsumerFactory;

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
    public NewTopic userLogTopic() {
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

    /**
     * 获取生产者实例
     */
    @Bean("customerKafkaTemplate")
    public KafkaTemplate<String, ProducerRecord<?, String>> customerKafkaTemplate() {
        return customerKafkaProducerFactory.kafkaTemplate(null, KafkaSendResultHandler.class);
    }

    /**
     * 在服务器加载Servlet的时候运行，并且只会被服务器调用一次
     */
    @PostConstruct
    public void consumerListener() {
        customerKafkaConsumerFactory.kafkaListenerContainer(
                "user-log-consumer-three",
                "kafka_consumer_group_user_log",
                ConsumerListener.class,
                TopicConstant.USER_LOG_TOPIC_MESSAGE,
                6)
                .startContainer();
    }

}
