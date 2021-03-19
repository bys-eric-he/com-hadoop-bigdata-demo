package com.hadoop.kafka;

import com.hadoop.kafka.common.KafkaUtils;
import lombok.extern.slf4j.Slf4j;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.List;

@Slf4j
@MapperScan(basePackages = "com.hadoop.kafka.mapper")
@SpringBootApplication
public class SpringBootHadoopKafkaApplication {
    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(SpringBootHadoopKafkaApplication.class, args);
        KafkaUtils kafkaUtils = context.getBean(KafkaUtils.class);
        List<String> queryAllTopic = kafkaUtils.queryAllTopic();
        log.info("---->当前系统所有消息主题：{}", queryAllTopic.toString());
    }
}
