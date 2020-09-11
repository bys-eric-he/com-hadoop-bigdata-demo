package com.hadoop.hive.controller;

import com.hadoop.hive.common.Result;
import com.hadoop.hive.common.ResultUtil;
import com.hadoop.hive.entity.car.*;
import com.hadoop.hive.service.CarService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api(tags = "CarController", description = "汽车销售数量分析")
@RestController
@RequestMapping("/api/v1/car")
public class CarController {

    @Autowired
    private CarService carService;

    @ApiOperation("获取销售数据列表")
    @RequestMapping(path = "/getCarList", method = RequestMethod.GET)
    public Result<List<Car>> getCarList() {
        List<Car> result = carService.getCarList();
        return ResultUtil.success(result);
    }

    @ApiOperation("统计各市、 区县的汽车销售的分布情况")
    @RequestMapping(path = "/getCarCityCountyDistribution", method = RequestMethod.GET)
    public Result<List<CarCityCountyDistribution>> getCarCityCountyDistribution() {
        List<CarCityCountyDistribution> result = carService.getCarCityCountyDistribution();
        return ResultUtil.success(result);
    }

    @ApiOperation("通过统计车辆不同用途的数量分布")
    @RequestMapping(path = "/getCarUsageQuantity", method = RequestMethod.GET)
    public Result<List<CarUsageQuantity>> getCarUsageQuantity() {
        List<CarUsageQuantity> result = carService.getCarUsageQuantity();
        return ResultUtil.success(result);
    }

    @ApiOperation("统计买车的男女比例")
    @RequestMapping(path = "/getCarGenderRatio", method = RequestMethod.GET)
    public Result<List<CarSexRatio>> getCarGenderRatio() {
        List<CarSexRatio> result = carService.getCarGenderRatio();
        return ResultUtil.success(result);
    }

    @ApiOperation("通过不同类型（品牌） 车销售情况， 来统计发动机型号和燃料种类")
    @RequestMapping(path = "/getCarModelFuel", method = RequestMethod.GET)
    public Result<List<CarModelFuel>> getCarModelFuel() {
        List<CarModelFuel> result = carService.getCarModelFuel();
        return ResultUtil.success(result);
    }

    @ApiOperation("每个月的汽车销售数量的比例")
    @RequestMapping(path = "/getCarMonthlySales", method = RequestMethod.GET)
    public Result<List<CarMonthlySales>> getCarMonthlySales() {
        List<CarMonthlySales> result = carService.getCarMonthlySales();
        return ResultUtil.success(result);
    }

    @ApiOperation("统计的车的所有权、 车辆类型和品牌的分布")
    @RequestMapping(path = "/getCarOwnershipTypeBrand", method = RequestMethod.GET)
    public Result<List<CarOwnershipTypeBrand>> getCarOwnershipTypeBrand() {
        List<CarOwnershipTypeBrand> result = carService.getCarOwnershipTypeBrand();
        return ResultUtil.success(result);
    }

}
