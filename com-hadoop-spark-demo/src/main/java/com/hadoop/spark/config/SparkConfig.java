package com.hadoop.spark.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * spark 配置类
 */
@Configuration
public class SparkConfig {

    @Autowired
    private SparkProperties sparkProperties;

    @Bean
    @ConditionalOnMissingBean(SparkConf.class)
    public SparkConf sparkConf() {
        return new SparkConf()
                // 设置模式为本地模式 [*] 为使用本机核数
                .setMaster(sparkProperties.getMaster())
                // 设置应用名
                .setAppName(sparkProperties.getAppName())
                .setSparkHome(sparkProperties.getSparkHome());
    }

    @Autowired
    private SparkConf sparkConf;

    @Bean
    @ConditionalOnMissingBean
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(sparkConf);
    }

    @Autowired
    private JavaSparkContext javaSparkContext;

    @Bean
    public SparkSession sqlContext() {
        return new SparkSession(javaSparkContext.sc());
    }

    /**
     * HiveContext也是已经过时的不推荐使用
     *
     * @return
     */
    @Bean
    @ConditionalOnMissingBean
    public HiveContext hiveContext() {
        return new HiveContext(javaSparkContext());
    }
}
