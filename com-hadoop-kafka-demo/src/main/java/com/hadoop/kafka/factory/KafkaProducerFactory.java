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
        //连接地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBootstrapServersConfig());
        //重试，0为不启用重试机制
        props.put(ProducerConfig.RETRIES_CONFIG, this.getRetriesConfig());
        //acks=0 ： 生产者在成功写入消息之前不会等待任何来自服务器的响应。
        //acks=1 ： 只要集群的首领节点收到消息，生产者就会收到一个来自服务器成功响应。
        //acks=all ：只有当所有参与复制的节点全部收到消息时，生产者才会收到一个来自服务器的成功响应。
        props.put(ProducerConfig.ACKS_CONFIG, this.getAcksConfig());
        //控制批处理大小，单位为字节
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, this.getBatchSizeConfig());
        //批量发送，延迟为1毫秒，启用该功能能有效减少生产者发送消息次数，从而提高并发量
        props.put(ProducerConfig.LINGER_MS_CONFIG, this.getLingerMSConfig());
        //键的序列化方式
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //值的序列化方式
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 设置幂等性
        // 如果用户还显式地指定了 acks 参数，那么还需要保证这个参数的值为 -1（all），如果不为 -1（这个参数的值默认为1）
        // 那么会报出 org.apache.kafka.common.config.ConfigException: Must set acks to all in order to use the idempotent producer. Otherwise we cannot guarantee idempotence.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        //指定分区策略
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,this.getPartitionStrategy());
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
