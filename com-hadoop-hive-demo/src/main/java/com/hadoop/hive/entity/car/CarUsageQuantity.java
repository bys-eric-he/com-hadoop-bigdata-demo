package com.hadoop.hive.entity.car;

import lombok.Data;

/**
 * 汽车用途和数量统计
 * select nature,count(number) as number from cars where nature!='' group by nature;
 */
@Data
public class CarUsageQuantity {
    /**
     * 使用性质
     */
    private String nature;
    /**
     * 数量
     */
    private Long number;
}
