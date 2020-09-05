package com.hadoop.hive.entity;

import lombok.Data;

import java.util.List;

/**
 * CREATE TABLE demo_database_enterprise.employee_complex_structure(name STRING,sa1ary FLOAT,subordinates ARRAY<STRING>,deductions MAP<STRING, FLOAT>,address STRUCT<street:STRING, city:STRING, state:STRING, zip:INT>) ROW FORMAT DELIMITED  FIELDS TERMINATED BY ','  COLLECTION ITEMS TERMINATED BY '_'  MAP KEYS TERMINATED BY ':'  LINES TERMINATED BY '\n';
 */
@Data
public class EmployeeComplexStructure {
    private String name;
    private Double sa1ary;
    private List<String> subordinates;

    private String deductions;
    private String address;
}
