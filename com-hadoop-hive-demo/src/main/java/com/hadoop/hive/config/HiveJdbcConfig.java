package com.hadoop.hive.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class HiveJdbcConfig {
    private static Logger logger = LoggerFactory.getLogger(HiveJdbcConfig.class);

    @Autowired
    private DataSourceProperties dataSourceProperties;

    @Bean(name = "hiveJdbcDataSource")
    @Qualifier("hiveJdbcDataSource")
    public DataSource dataSource() {
        DataSource dataSource = new DataSource();
        dataSource.setUrl(dataSourceProperties.getHive().get("url"));
        dataSource.setUsername(dataSourceProperties.getHive().get("username"));
        dataSource.setPassword(dataSourceProperties.getHive().get("password"));
        dataSource.setDriverClassName(dataSourceProperties.getHive().get("driver-class-name"));
        logger.debug("----------Hive 数据源注入 Successfully-----------");
        return dataSource;
    }

    @Bean(name = "hiveJdbcTemplate")
    public JdbcTemplate hiveJdbcTemplate(@Qualifier("hiveJdbcDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
