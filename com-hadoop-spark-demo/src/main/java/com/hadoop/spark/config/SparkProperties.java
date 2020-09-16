package com.hadoop.spark.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Service;

@Service
@ConfigurationProperties(prefix = "spark")
@Data
public class SparkProperties {
    private String appName;

    private String master;

    private String sparkHome;
}
