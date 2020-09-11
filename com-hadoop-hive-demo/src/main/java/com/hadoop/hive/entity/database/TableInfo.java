package com.hadoop.hive.entity.database;

import lombok.Data;

/**
 * Hive 数据表结构信息
 */
@Data
public class TableInfo {
    /**
     * 字段名称
     */
    private String columnName;
    /**
     * 字段类型
     */
    private String columnType;
    /**
     * 字段描述
     */
    private String columnComment;
}
