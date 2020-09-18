package com.hadoop.kafka.controller;

import com.hadoop.kafka.common.Result;
import com.hadoop.kafka.common.ResultUtil;
import com.hadoop.kafka.model.UserOrder;
import com.hadoop.kafka.producer.UserOrderProducer;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "UserOrderController", description = "用户订单API服务")
@RestController
@RequestMapping("/api/v1/user-order")
public class UserOrderController {

    @Autowired
    private UserOrderProducer userOrderProducer;

    @ApiOperation("发送订单到后端")
    @RequestMapping(path = "/send", method = RequestMethod.POST)
    public Result<Object> getCarCityCountyDistribution(UserOrder userOrder) {
        userOrderProducer.sendOrder(userOrder);
        return ResultUtil.success();
    }
}
