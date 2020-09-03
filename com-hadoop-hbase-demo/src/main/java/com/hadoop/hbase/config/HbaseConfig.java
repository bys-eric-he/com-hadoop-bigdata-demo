package com.hadoop.hbase.config;


import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.Set;

/**
 * Hbase配置类
 *
 * @author heyong
 * @Date 2020年09月01日
 */
@Configuration
@EnableConfigurationProperties(HbaseProperties.class)
public class HbaseConfig {

    @Autowired
    private HbaseProperties properties;

    /**
     * 配置对象
     *
     * @return
     */
    @Bean("hbaseConfiguration")
    @Qualifier("hbaseConfiguration")
    public org.apache.hadoop.conf.Configuration configuration() {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        Map<String, String> config = properties.getConfig();
        Set<String> keySet = config.keySet();
        for (String key : keySet) {
            configuration.set(key, config.get(key));
        }
        return configuration;
    }
}