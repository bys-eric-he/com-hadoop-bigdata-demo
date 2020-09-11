package com.hadoop.hive.repository;

import com.hadoop.hive.annotation.LogAspect;
import com.hadoop.hive.entity.car.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Slf4j
@Repository
public class CarRepository extends HiveBaseJDBCTemplate {
    /**
     * 获取销售数据列表
     *
     * @param sql
     * @return
     */
    @LogAspect(value = "getCarList")
    public List<Car> getCarList(String sql) {
        return this.getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(Car.class));
    }

    /**
     * 通过统计车辆不同用途的数量分布
     *
     * @param sql
     * @return
     */
    @LogAspect(value = "getCarUsageQuantity")
    public List<CarUsageQuantity> getCarUsageQuantity(String sql) {
        return this.getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(CarUsageQuantity.class));
    }

    /**
     * 统计各市、 区县的汽车销售的分布情况
     *
     * @param sql
     * @return
     */
    @LogAspect(value = "getCarCityCountyDistribution")
    public List<CarCityCountyDistribution> getCarCityCountyDistribution(String sql) {
        return this.getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(CarCityCountyDistribution.class));
    }

    /**
     * 统计买车的男女比例
     *
     * @param sql
     * @return
     */
    @LogAspect(value = "getCarGenderRatio")
    public List<CarSexRatio> getCarGenderRatio(String sql) {
        return this.getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(CarSexRatio.class));
    }

    /**
     * 通过不同类型（品牌） 车销售情况， 来统计发动机型号和燃料种类
     *
     * @param sql
     * @return
     */
    @LogAspect(value = "getCarModelFuel")
    public List<CarModelFuel> getCarModelFuel(String sql) {
        return this.getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(CarModelFuel.class));
    }

    /**
     * 每个月的汽车销售数量的比例
     *
     * @param sql
     * @return
     */
    @LogAspect(value = "getCarMonthlySales")
    public List<CarMonthlySales> getCarMonthlySales(String sql) {
        return this.getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(CarMonthlySales.class));
    }

    /**
     * 统计的车的所有权、 车辆类型和品牌的分布
     *
     * @param sql
     * @return
     */
    @LogAspect(value = "getCarOwnershipTypeBrand")
    public List<CarOwnershipTypeBrand> getCarOwnershipTypeBrand(String sql) {
        return this.getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(CarOwnershipTypeBrand.class));
    }
}
