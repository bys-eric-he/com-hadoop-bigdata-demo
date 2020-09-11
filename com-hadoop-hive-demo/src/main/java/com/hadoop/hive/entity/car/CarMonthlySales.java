package com.hadoop.hive.entity.car;

import lombok.Data;

/**
 * 每个月的汽车销售数量的比例
 * select month,round(summon/sumcount,2)as per
 * from (select month,count(number) as summon from cars where
 * month is not null group by month) as a,
 * (select count(number) as sumcount from cars) as b;
 */
@Data
public class CarMonthlySales {
    /**
     * 月份
     */
    private int month;
    /**
     * 比例
     */
    private double per;
}
