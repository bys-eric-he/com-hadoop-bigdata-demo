package com.hadoop.kafka.controller;

import com.hadoop.kafka.common.Result;
import com.hadoop.kafka.common.ResultUtil;
import com.hadoop.kafka.model.UserOrder;
import com.hadoop.kafka.producer.UserOrderProducer;
import com.hadoop.kafka.service.UserOrderService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

@Api(value = "/api/v1/user-order", tags = "用户订单API服务")
@RestController
@RequestMapping("/api/v1/user-order")
public class UserOrderController {

    @Autowired
    private UserOrderProducer userOrderProducer;

    @Resource
    @Qualifier("userOrderService")
    private UserOrderService userOrderService;

    @ApiOperation("发送订单到后端")
    @RequestMapping(path = "/send", method = RequestMethod.POST)
    public Result<Object> getCarCityCountyDistribution(UserOrder userOrder) {
        userOrderProducer.sendOrder(userOrder);
        return ResultUtil.success();
    }

    @ApiOperation("获取所有订单列表")
    @RequestMapping(path = "/list", method = RequestMethod.GET)
    public Result<List<UserOrder>> getAllOrders() {
        List<UserOrder> userOrders = userOrderService.getAllOrders();

        return ResultUtil.success(userOrders);
    }
}
