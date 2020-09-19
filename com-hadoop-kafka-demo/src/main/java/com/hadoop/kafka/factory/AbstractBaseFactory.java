package com.hadoop.kafka.factory;

import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;

@Data
public abstract class AbstractBaseFactory {
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
    @Value("${spring.kafka.producer.acks}")
    private String acksConfig;

    /**
     * 设置自动提交offset
     */
    @Value("${spring.kafka.consumer.enable-auto-commit}")
    private Boolean EnableAutoCommitConfig;

    /**
     * 控制单次调用call方法能够返回的记录数量，帮助控制在轮询里需要处理的数据量
     */
    @Value("${spring.kafka.consumer.properties.max.poll.records}")
    private Integer MaxPollRecordsConfig;


}
