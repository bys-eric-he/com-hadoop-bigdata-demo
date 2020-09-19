package com.hadoop.kafka.factory;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * 生产者工厂
 */
@Configuration
public class KafkaProducerFactory {
    @Autowired
    private ApplicationContext context;
    /**
     * 指定kafka server的地址，集群可配多个，中间，逗号隔开
     */
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServersConfig;
    /**
     * 失败重试发送的次数
     */
    @Value("${spring.kafka.producer.retries}")
    private Integer retriesConfig;
    /**
     * 失败重试发送的次数
     */
    @Value("${spring.kafka.producer.retries}")
    private String acksConfig;

    /**
     * 获取生产者工厂
     */
    public ProducerFactory<String, ProducerRecord<?, String>> producerFactory() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        props.put(ProducerConfig.RETRIES_CONFIG, retriesConfig);
        props.put(ProducerConfig.ACKS_CONFIG, acksConfig);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * 注册生产者实例
     *
     * @param topicName
     * @param clazz
     * @return
     */
    public KafkaTemplate<String, ProducerRecord<?, String>> kafkaTemplate(String topicName, Class clazz) {
        KafkaTemplate<String, ProducerRecord<?, String>> template = new KafkaTemplate<>(producerFactory(), Boolean.FALSE);
        if (!StringUtils.isEmpty(topicName)) {
            template.setDefaultTopic(topicName);
        }
        template.setProducerListener((ProducerListener<String, ProducerRecord<?, String>>) context.getBean(clazz));
        return template;
    }
}
