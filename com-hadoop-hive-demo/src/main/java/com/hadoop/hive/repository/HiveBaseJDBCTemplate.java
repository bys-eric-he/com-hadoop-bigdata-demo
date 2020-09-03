package com.hadoop.hive.repository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * 注入hive数据源
 *
 * @author heyong
 * @Date 2020年09月01日
 */
@Slf4j
public class HiveBaseJDBCTemplate {
    private JdbcTemplate jdbcTemplate;

    public JdbcTemplate getJdbcTemplate() {
        return this.jdbcTemplate;
    }

    @Autowired
    public void setJdbcTemplate(@Qualifier("hiveDruidDataSource") DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Autowired
    @Qualifier("hiveJdbcDataSource")
    private org.apache.tomcat.jdbc.pool.DataSource jdbcDataSource;

    public org.apache.tomcat.jdbc.pool.DataSource getJdbcDataSource() {
        return this.jdbcDataSource;
    }
}
