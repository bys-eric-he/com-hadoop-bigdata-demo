package com.hadoop.hive.service;

public interface ETLService {
    /**
     * 执行统计语句
     *
     * @param sql
     */
    void execute(String sql);
}
