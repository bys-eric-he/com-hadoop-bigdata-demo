package com.hadoop.kafka.factory;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
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
@Slf4j
@Configuration("customerKafkaProducerFactory")
public class KafkaProducerFactory extends AbstractBaseFactory {

    /**
     * 获取生产者工厂
     */
    public ProducerFactory<String, ProducerRecord<?, String>> producerFactory() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBootstrapServersConfig());
        props.put(ProducerConfig.RETRIES_CONFIG, this.getRetriesConfig());
        props.put(ProducerConfig.ACKS_CONFIG, this.getAcksConfig());
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
        template.setProducerListener((ProducerListener<String, ProducerRecord<?, String>>) this.getContext().getBean(clazz));

        log.info("------------------注册自定义生产者例完成----------------");
        return template;
    }
}
