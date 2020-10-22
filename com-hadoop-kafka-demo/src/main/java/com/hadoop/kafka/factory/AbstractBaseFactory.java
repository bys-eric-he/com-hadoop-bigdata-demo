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
     * 控制批处理大小，单位为字节
     */
    @Value("${spring.kafka.producer.batch-size}")
    private String batchSizeConfig;

    /**
     * 批量发送，延迟为1毫秒，启用该功能能有效减少生产者发送消息次数，从而提高并发量
     */
    @Value("${spring.kafka.producer.linger-ms}")
    private String lingerMSConfig;

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

    /**
     * 默认消费者group id
     */
    @Value("${spring.kafka.consumer.group-id}")
    private String groupID;

    /**
     * Session过期时间
     */
    @Value("${spring.kafka.consumer.session-timeout}")
    private String sessionTimeOut;
}
