package com.hadoop.spark.service;

import com.hadoop.spark.config.MySQLConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Properties;

@Slf4j
@Component
public class ETLService {
    @Autowired
    private SparkSession session;

    @Autowired
    private MySQLConfig mySQLConfig;

    @PostConstruct
    public void etl() throws Exception {
        //原表
        Properties prod = mySQLConfig.getConnectionProperties();
        // 落地表配置
        Properties local = mySQLConfig.getConnectionProperties();

        //writeLive(prod, local, session);
    }

    private void writeLive(Properties prod, Properties local, SparkSession session) {
        log.info("---->开始执行ETL<----");
        long start = System.currentTimeMillis();

        Dataset d1 = session.read()
                .option(JDBCOptions.JDBC_BATCH_FETCH_SIZE(), 1000)
                .jdbc(mySQLConfig.getUrl(), mySQLConfig.getTable(), prod)
                .selectExpr("id", "user_name", "password")
                .persist(StorageLevel.MEMORY_ONLY_SER());
        d1.createOrReplaceTempView("userTemp");

        //从临时表中读取数据
        Dataset d2 = session.sql("select * from userTemp");
        //将读取到内存的表处理后再次写到mysql中 local 配置为目的库配置
        d2.write().mode(SaveMode.Append)
                .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE(), 1000)
                .jdbc(mySQLConfig.getUrl(), "user_2", local);
        long end = System.currentTimeMillis();

        log.info("------>耗时<------" + (end - start) / 1000L);
        //运行完后释放内存
        d1.unpersist(true);
        d2.unpersist(true);
    }

}
