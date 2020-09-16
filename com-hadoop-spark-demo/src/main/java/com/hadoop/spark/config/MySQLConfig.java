package com.hadoop.spark.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
@Configuration
public class MySQLConfig {
    private String table;
    private String url;
    private Properties connectionProperties;

    /**
     * 初始化MySQL连接属性
     */
    @PostConstruct
    public void init() {
        Properties properties = new Properties();
        InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream("jdbc.properties");
        try {
            properties.load(resourceAsStream);
            setUrl(properties.getProperty("db.url"));
            setTable(properties.getProperty("db.table"));
            //考虑多数据源的情况，另外创建properties传入
            Properties connectionProperties = new Properties();
            connectionProperties.setProperty("user", properties.getProperty("db.user"));
            connectionProperties.setProperty("password", properties.getProperty("db.password"));
            connectionProperties.setProperty("url", properties.getProperty("db.url"));
            setConnectionProperties(connectionProperties);
        } catch (IOException e) {
            log.info("---读取配置文件失败---");
        }

    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Properties getConnectionProperties() {
        return connectionProperties;
    }

    public void setConnectionProperties(Properties connectionProperties) {

        this.connectionProperties = connectionProperties;
    }
}
