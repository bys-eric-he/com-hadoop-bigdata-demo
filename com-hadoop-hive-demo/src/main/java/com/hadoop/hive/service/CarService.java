package com.hadoop.hive.service;

import com.hadoop.hive.entity.car.*;

import java.util.List;

/**
 * 汽车销售数据统计
 */
public interface CarService {
    /**
     * 获取销售数据列表
     *
     * @return
     */
    List<Car> getCarList();

    /**
     * 统计各市、 区县的汽车销售的分布情况
     *
     * @return
     */
    List<CarCityCountyDistribution> getCarCityCountyDistribution();

    /**
     * 汽车用途和数量统计
     *
     * @return
     */
    List<CarUsageQuantity> getCarUsageQuantity();

    /**
     * 统计买车的男女比例
     *
     * @return
     */
    List<CarSexRatio> getCarGenderRatio();

    /**
     * 通过不同类型（品牌） 车销售情况， 来统计发动机型号和燃料种类
     *
     * @return
     */
    List<CarModelFuel> getCarModelFuel();

    /**
     * 每个月的汽车销售数量的比例
     *
     * @return
     */
    List<CarMonthlySales> getCarMonthlySales();

    /**
     * 统计的车的所有权、 车辆类型和品牌的分布
     *
     * @return
     */
    List<CarOwnershipTypeBrand> getCarOwnershipTypeBrand();
}
