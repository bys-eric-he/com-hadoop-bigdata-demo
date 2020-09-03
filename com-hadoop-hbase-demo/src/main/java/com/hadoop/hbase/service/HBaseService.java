package com.hadoop.hbase.service;

import java.io.IOException;
import java.util.List;

public interface HBaseService {

    /**
     * 检查表是否已经存在
     *
     * @param tableName 表名
     * @return
     * @throws IOException
     */
    boolean tableExists(String tableName) throws IOException;

    /**
     * 创建表
     *
     * @param tableName      表名
     * @param columnFamilies 列族（数组）
     */
    void createTable(String tableName, String[] columnFamilies) throws Exception;

    /**
     * 创建表
     *
     * @param tableName      表名
     * @param columnFamilies 列族（数组）
     * @param isExistsRemove 如果表存在是否删除
     */
    void createTable(String tableName, String[] columnFamilies, Boolean isExistsRemove) throws IOException;

    /**
     * 插入记录（单行单列族-多列多值）
     *
     * @param tableName      表名
     * @param row            行名
     * @param columnFamilies 列族名
     * @param columns        列名（数组）
     * @param values         值（数组）（且需要和列一一对应）
     */
    void insertRecords(String tableName, String row, String columnFamilies, String[] columns, String[] values) throws IOException;

    /**
     * 插入记录（单行单列族-单列单值）
     *
     * @param tableName    表名
     * @param row          行名
     * @param columnFamily 列族名
     * @param column       列名
     * @param value        值
     */
    void insertOneRecord(String tableName, String row, String columnFamily, String column, String value) throws IOException;

    /**
     * 删除一行记录
     *
     * @param tableName 表名
     * @param rowKey    行名
     */
    void deleteRow(String tableName, String rowKey) throws IOException;

    /**
     * 删除单行单列族记录
     *
     * @param tableName    表名
     * @param rowKey       行名
     * @param columnFamily 列族名
     */
    void deleteColumnFamily(String tableName, String rowKey, String columnFamily) throws IOException;

    /**
     * 删除单行单列族单列记录
     *
     * @param tableName    表名
     * @param rowKey       行名
     * @param columnFamily 列族名
     * @param column       列名
     */
    void deleteColumn(String tableName, String rowKey, String columnFamily, String column) throws IOException;

    /**
     * 查找一行记录
     *
     * @param tableName 表名
     * @param rowKey    行名
     */
    String selectRow(String tableName, String rowKey) throws IOException;

    /**
     * 查找单行单列族单列记录
     *
     * @param tableName    表名
     * @param rowKey       行名
     * @param columnFamily 列族名
     * @param column       列名
     * @return
     */
    String selectValue(String tableName, String rowKey, String columnFamily, String column) throws IOException;

    /**
     * 查找单行单列族单列记录
     *
     * @param tableName    表名
     * @param rowKey       行名
     * @param columnFamily 列族名
     * @param column       列名
     * @param maxVersions  获取多少个版本的数据
     * @return
     */
    String selectValue(String tableName, String rowKey, String columnFamily, String column, int maxVersions) throws Exception;
    /**
     * 查询表中所有行（Scan方式）
     *
     * @param tableName
     * @return
     */
    String scanAllRecord(String tableName) throws IOException;

    /**
     * 根据rowKey关键字查询报告记录
     *
     * @param tableName  表名
     * @param rowKeyword rowKey关键字
     * @return
     */
    List<Object> scanReportDataByRowKeyword(String tableName, String rowKeyword) throws IOException;

    /**
     * 根据rowKey关键字和时间戳范围查询报告记录
     *
     * @param tableName  表名
     * @param rowKeyword rowKey关键字
     * @param minStamp 最小时间
     * @param maxStamp 最大时间
     * @return
     */
    List<Object> scanReportDataByRowKeywordTimestamp(String tableName, String rowKeyword, Long minStamp, Long maxStamp) throws IOException;

    /**
     * 删除表操作
     *
     * @param tableName 表名
     */
    void deleteTable(String tableName) throws IOException;

}
