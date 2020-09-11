package com.hadoop.hive.service.impl;

import com.hadoop.hive.entity.car.*;
import com.hadoop.hive.repository.CarRepository;
import com.hadoop.hive.service.CarService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 汽车销售数据统计
 */
@Slf4j
@Service
public class CarServiceImpl implements CarService {

    @Autowired
    private CarRepository carRepository;

    /**
     * 获取销售数据列表
     *
     * @return
     */
    @Override
    public List<Car> getCarList() {
        String sql = "select province,month,city,county,year,cartype,productor,brand,mold,owner,nature,number,ftype,outv,power,fuel,length,width,height,xlength,xwidth,xheight,count,base,front,norm,tnumber,total,curb,hcurb,passenger,zhcurb,business,dtype,fmold,fbusiness,name,age,sex from cars";
        return carRepository.getCarList(sql);
    }

    /**
     * 统计各市、 区县的汽车销售的分布情况
     *
     * @return
     */
    @Override
    public List<CarCityCountyDistribution> getCarCityCountyDistribution() {
        String sql = "select city,county,count(number)as number from cars group by city,county";
        return carRepository.getCarCityCountyDistribution(sql);
    }

    /**
     * 汽车用途和数量统计
     *
     * @return
     */
    @Override
    public List<CarUsageQuantity> getCarUsageQuantity() {
        String sql = "select nature,count(number) as number from cars where nature!='' group by nature";
        return carRepository.getCarUsageQuantity(sql);
    }

    /**
     * 统计买车的男女比例
     *
     * @return
     */
    @Override
    public List<CarSexRatio> getCarGenderRatio() {
        String sql = "select sex,round((sumsex/sumcount),2) as sexper from (select sex,count(number) as sumsex from cars where sex!=''group by sex) as a,(select count(number) as sumcount from cars where  sex !='') as b";
        return carRepository.getCarGenderRatio(sql);
    }

    /**
     * 通过不同类型（品牌） 车销售情况， 来统计发动机型号和燃料种类
     *
     * @return
     */
    @Override
    public List<CarModelFuel> getCarModelFuel() {
        String sql = "select brand,mold,collect_set(ftype) as fTypes,collect_set(fuel) as fuels from cars where brand is not null and brand != '' and mold is not null and mold != ''  group by brand,mold";
        return carRepository.getCarModelFuel(sql);
    }

    /**
     * 每个月的汽车销售数量的比例
     *
     * @return
     */
    @Override
    public List<CarMonthlySales> getCarMonthlySales() {
        String sql = "select month,round(summon/sumcount,2)as per from (select month,count(number) as summon from cars where month is not null group by month) as a,(select count(number) as sumcount from cars) as b";
        return carRepository.getCarMonthlySales(sql);
    }

    /**
     * 统计的车的所有权、 车辆类型和品牌的分布
     *
     * @return
     */
    @Override
    public List<CarOwnershipTypeBrand> getCarOwnershipTypeBrand() {
        String sql = "select owner,mold,brand,count(number) as number from cars group by owner,mold,brand";
        return carRepository.getCarOwnershipTypeBrand(sql);
    }
}
