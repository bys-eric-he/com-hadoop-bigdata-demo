package com.hadoop.hive.repository;

import com.hadoop.hive.annotation.LogAspect;
import com.hadoop.hive.entity.database.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Hive 数据仓库通用持久化层
 */
@Repository
public class HiveRepository extends HiveBaseJDBCTemplate {

    private final static Logger logger = LoggerFactory.getLogger(HiveRepository.class);

    /**
     * 执行SQL
     *
     * @param sql
     */
    public void execute(String sql) {
        this.getJdbcTemplate().execute(sql);
        logger.info("--->Running: " + sql);
    }

    /**
     * 获取所以数据信息
     *
     * @param sql
     * @return
     */
    @LogAspect(value = "queryForList")
    public List<Map<String, Object>> queryForList(String sql) {
        return this.getJdbcTemplate().queryForList(sql);
    }

    /**
     * 列举当前Hive库中的所有数据表
     */
    public List<String> listAllTables() throws SQLException {
        List<String> tables = new ArrayList<>();
        Statement statement = this.getJdbcDataSource().getConnection().createStatement();
        String sql = "show tables";
        logger.info("--->Running: " + sql);
        ResultSet res = statement.executeQuery(sql);
        while (res.next()) {
            tables.add(res.getString(1));
        }
        return tables;
    }

    /**
     * 创建新表
     *
     * @param sql
     * @return
     */
    public String createTable(String sql) {
        logger.info("-->Running: " + sql);
        String result = "创建表成功...";
        try {
            this.getJdbcTemplate().execute(sql);
        } catch (DataAccessException dae) {
            result = "********创建表异常，异常信息-> " + dae.getMessage();
            logger.error(result);
        }
        return result;

    }

    /**
     * 将Hive服务器本地文档中的数据加载到Hive表中
     *
     * @param filePath  /home/hive_data/user_sample.txt
     * @param tableName user_sample"
     * @return
     */
    public String loadIntoTable(String filePath, String tableName) {
        String result = "加载数据到表成功...";
        try {
            String sql = "load data local inpath '" + filePath + "' into table '" + tableName + "'";
            this.getJdbcTemplate().execute(sql);
            logger.info("-->{},\n 执行SQL->{}", result, sql);
        } catch (DataAccessException dae) {
            result = "********加载数据到表失败，错误信息-> " + dae.getMessage();
            logger.error(result);
        }
        return result;
    }

    /**
     * 向Hive表中添加数据
     *
     * @param sql
     * @return
     */
    public String insertIntoTable(String sql) {
        String result = "插入数据到表成功!";
        try {
            this.getJdbcTemplate().execute(sql);
            logger.info("-->{},\n执行SQL->{}", result, sql);
        } catch (DataAccessException dae) {
            result = "********插入数据到表失败, 异常信息-> " + dae.getMessage();
            logger.error(result);
        }
        return result;
    }

    /**
     * 删除表
     *
     * @param tableName
     * @return
     */
    public String drop(String tableName) {
        String sql = "DROP TABLE IF EXISTS " + tableName;
        String result = "删除表成功...";
        logger.info("-->Running: " + sql);
        try {
            this.getJdbcTemplate().execute(sql);
            logger.info("-->{},\n 执行SQL->{}", result, sql);
        } catch (DataAccessException dae) {
            result = "********删除表发生异常, 异常信息-> " + dae.getMessage();
            logger.error(result);
        }
        return result;
    }

    /**
     * 查询Hive库中的某张数据表字段信息
     *
     * @param tableName
     * @return
     * @throws SQLException
     */
    public List<String> describeTable(String tableName) throws SQLException {
        List<String> list = new ArrayList<>();
        Statement statement = this.getJdbcDataSource().getConnection().createStatement();
        String sql = "describe " + tableName;
        logger.info("-->Running: " + sql);
        ResultSet res = statement.executeQuery(sql);
        while (res.next()) {
            list.add(res.getString(1));
        }
        return list;
    }

    /**
     * 获取表结构详细信息
     *
     * @param tableName 表名
     * @return
     */
    public List<TableInfo> describeTableInfo(String tableName) throws SQLException {
        List<TableInfo> tableInfoList = new ArrayList<>();

        Statement statement = this.getJdbcDataSource().getConnection().createStatement();
        String sql = "describe " + tableName;
        logger.info("-->Running: " + sql);
        ResultSet res = statement.executeQuery(sql);

        while (res.next()) {
            TableInfo tableInfo = new TableInfo();
            tableInfo.setColumnName(res.getString(1));
            tableInfo.setColumnType(res.getString(2));
            tableInfo.setColumnComment(res.getString(3));
            tableInfoList.add(tableInfo);
        }

        return tableInfoList;
    }

    /**
     * 查询指定tableName表中的数据
     *
     * @param tableName 表名
     * @return
     * @throws SQLException
     */
    public List<String> selectFromTable(String tableName) throws SQLException {
        Statement statement = this.getJdbcDataSource().getConnection().createStatement();
        String sql = "select * from " + tableName;
        logger.info("-->Running: " + sql);
        ResultSet res = statement.executeQuery(sql);
        List<String> list = new ArrayList<>();
        int count = res.getMetaData().getColumnCount();

        while (res.next()) {
            StringBuilder str = new StringBuilder();
            for (int i = 1; i < count; i++) {
                str.append(res.getString(i)).append(" ");
            }
            str.append(res.getString(count));
            logger.info(str.toString());
            list.add(str.toString());
        }
        return list;
    }
}
