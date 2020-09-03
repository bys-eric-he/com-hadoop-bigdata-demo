package com.hadoop.hbase.repository;

import com.hadoop.hbase.component.SpringContextHolder;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 控制依赖顺序，保证springContextHolder类在之前已经加载
 */
@DependsOn("springContextHolder")
public class HBaseRepository {

    //设置hbase连接池
    private static ExecutorService pool = Executors.newScheduledThreadPool(20);
    private static Connection connection = null;
    private static HBaseRepository instance = null;
    private static Admin admin = null;

    private final Logger logger = LoggerFactory.getLogger(HBaseRepository.class);

    private HBaseRepository() {
        if (connection == null) {
            try {
                org.apache.hadoop.conf.Configuration configuration = SpringContextHolder.getBean("hbaseConfiguration");
                connection = ConnectionFactory.createConnection(configuration, pool);
                admin = connection.getAdmin();
            } catch (IOException e) {
                logger.error("*****HBaseRepository实例初始化失败！错误信息为：" + e.getMessage(), e);
            }
        }
    }

    /**
     * 简单单例方法，如果autowired自动注入就不需要此方法
     *
     * @return
     */
    public static synchronized HBaseRepository getInstance() {
        if (instance == null) {
            instance = new HBaseRepository();
        }
        return instance;
    }

    /**
     * HBase 管理 API
     *
     * @return
     */
    public Admin getAdmin() {
        if (admin == null) {
            logger.error("*****获取 HBase 管理 Admin 实例失败！ admin is NULL!");
        }
        return admin;
    }

    /**
     * HBase 连接
     *
     * @return
     */
    public Connection getConnection() {
        if (connection == null) {
            logger.error("*****获取 HBase 连接 Connection 实例失败! connection is NULL!");
        }
        return connection;
    }
}
