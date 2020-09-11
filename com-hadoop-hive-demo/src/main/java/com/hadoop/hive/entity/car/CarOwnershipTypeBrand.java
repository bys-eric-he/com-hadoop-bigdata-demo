package com.hadoop.hive.entity.car;

import lombok.Data;

/**
 * 统计的车的所有权、 车辆类型和品牌的分布
 * select ower,mold,brand,count(number) as number from cars
 * group by ower,mold,brand;
 */
@Data
public class CarOwnershipTypeBrand {
    /**
     * 所有权
     */
    private String owner;
    /**
     * 车辆类型
     */
    private String mold;
    /**
     * 品牌
     */
    private String brand;
    /**
     * 分布数量
     */
    private int number;
}
