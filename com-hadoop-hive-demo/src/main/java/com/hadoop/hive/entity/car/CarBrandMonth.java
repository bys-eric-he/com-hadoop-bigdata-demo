package com.hadoop.hive.entity.car;

import lombok.Data;

/**
 * 统计不同品牌的车在每个月的销售量分布
 * select brand,month,count(number) as number from cars where brand !='' and month != '' group by brand,month;
 */
@Data
public class CarBrandMonth {
    /**
     * 品牌
     */
    private String brand;
    /**
     * 月份
     */
    private int month;
    /**
     * 数量
     */
    private int number;
}
