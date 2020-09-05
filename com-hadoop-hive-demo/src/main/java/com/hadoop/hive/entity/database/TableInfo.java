package com.hadoop.hive.entity.database;

import lombok.Data;

@Data
public class TableInfo {
    private String columnName;
    private String columnType;
    private String columnComment;
}
