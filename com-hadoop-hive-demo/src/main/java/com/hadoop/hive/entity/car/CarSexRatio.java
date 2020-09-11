package com.hadoop.hive.entity.car;

import lombok.Data;

/**
 * 统计买车的男女比例
 * 以下SQL属于 hive 非等值连接， 需要设置hive为nonstrict模式 set hive.mapred.mode=nonstrict;
 * <p>
 * select sex,round((sumsex/sumcount),2) as sexper from
 * (select sex,count(number) as sumsex from cars where sex!=''
 * group by sex) as a,
 * (select count(number) as sumcount from cars where  sex !='') as b;
 */
@Data
public class CarSexRatio {
    /**
     * 性别
     */
    private String sex;
    /**
     * 性别比例
     */
    private double sexper;
}
