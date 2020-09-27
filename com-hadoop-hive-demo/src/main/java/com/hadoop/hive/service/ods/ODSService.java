package com.hadoop.hive.service.ods;

/**
 * 日志ODS原始层服务
 */
public interface ODSService<T> {
    /**
     * 保存日志包原始数据
     *
     * @param t
     */
    void insert(T t);
}
