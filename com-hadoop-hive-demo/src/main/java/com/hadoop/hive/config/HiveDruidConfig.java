package com.hadoop.hive.config;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.alibaba.druid.pool.DruidDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;


/**
 * 配置hive数据源
 *
 * @author heyong
 * @Date 2020年09月01日
 */
@Configuration
@EnableConfigurationProperties({DataSourceProperties.class})
public class HiveDruidConfig {

    private static Logger logger = LoggerFactory.getLogger(HiveDruidConfig.class);

    @Autowired
    private DataSourceProperties dataSourceProperties;

    @Bean("hiveDruidDataSource")
    @Qualifier("hiveDruidDataSource")
    public DataSource dataSource() {
        DruidDataSource datasource = new DruidDataSource();
        try {
            //配置数据源属性
            datasource.setUrl(dataSourceProperties.getHive().get("url"));
            datasource.setUsername(dataSourceProperties.getHive().get("username"));
            datasource.setPassword(dataSourceProperties.getHive().get("password"));
            datasource.setDriverClassName(dataSourceProperties.getHive().get("driver-class-name"));

            //配置统一属性
            datasource.setInitialSize((Integer.parseInt(dataSourceProperties.getPool().get("initial-size"))));
            datasource.setMinIdle(Integer.parseInt(dataSourceProperties.getPool().get("min-idle")));
            datasource.setMaxActive(Integer.parseInt(dataSourceProperties.getPool().get("max-active")));
            datasource.setMaxWait(Long.parseLong(dataSourceProperties.getPool().get("max-wait")));
            datasource.setTimeBetweenEvictionRunsMillis(Long.parseLong(dataSourceProperties.getPool().get("time-between-eviction-runs-millis")));
            datasource.setMinEvictableIdleTimeMillis(Long.parseLong(dataSourceProperties.getPool().get("min-evictable-idle-time-millis")));
            datasource.setValidationQuery(dataSourceProperties.getPool().get("validation-query"));
            datasource.setTestWhileIdle(Boolean.parseBoolean(dataSourceProperties.getPool().get("test-while-idle")));
            datasource.setTestOnBorrow(Boolean.parseBoolean(dataSourceProperties.getPool().get("test-on-borrow")));
            datasource.setTestOnReturn(Boolean.parseBoolean(dataSourceProperties.getPool().get("test-on-return")));
            datasource.setPoolPreparedStatements(Boolean.parseBoolean(dataSourceProperties.getPool().get("pool-prepared-statements")));

            datasource.setFilters(dataSourceProperties.getPool().get("filters"));
            logger.info("------------初始化 DruidDataSource 数据源信息完成--------------");
        } catch (SQLException e) {
            logger.error("**************初始化 DruidDataSource 数据源信息 异常.************", e);
        }
        return datasource;
    }

}