package com.hadoop.hive.entity.car;

import lombok.Data;

/**
 * 统计各市、 区县的汽车销售的分布情况
 * select city,county,count(number)as number from cars group by city,county;
 */
@Data
public class CarCityCountyDistribution {
    /**
     * 市
     */
    private String city;
    /**
     * 区、县
     */
    private String county;
    /**
     * 数量
     */
    private int number;
}
