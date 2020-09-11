package com.hadoop.hive.entity.car;

import lombok.Data;

import java.util.List;

/**
 * 通过不同类型（品牌） 车销售情况， 来统计发动机型号和燃料种类
 * select brand,mold,collect_set(ftype) as fTypes,collect_set(fuel) as fuels from cars where brand is not null and brand != '' and mold is not null and mold != ''  group by brand,mold;
 */
@Data
public class CarModelFuel {
    /**
     * 品牌
     */
    private String brand;
    /**
     * 车辆类型
     */
    private String mold;
    /**
     * 发动机型号
     */
    private List<String> fTypes;
    /**
     * 燃料种类
     */
    private List<String> fuels;
}
