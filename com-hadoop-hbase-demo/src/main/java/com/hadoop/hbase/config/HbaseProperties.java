package com.hadoop.hbase.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * Hbase配置属性
 */
@ConfigurationProperties(prefix = HbaseProperties.HC, ignoreUnknownFields = false)
public class HbaseProperties {
    final static String HC = "hbase";
    private Map<String, String> config;

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }
}
