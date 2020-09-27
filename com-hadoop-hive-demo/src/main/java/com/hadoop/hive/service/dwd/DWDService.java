package com.hadoop.hive.service.dwd;

/**
 * ODS层清洗抽取数据到DWD层
 */
public interface DWDService {
    /**
     * 执行统计语句
     *
     * @param sql
     */
    void execute(String sql);
}
