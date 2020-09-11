package com.hadoop.hive.entity.employee;

import com.hadoop.hive.entity.BaseInfo;
import lombok.Data;

@Data
public class Employee {
    private int id;
    private BaseInfo info;
}
