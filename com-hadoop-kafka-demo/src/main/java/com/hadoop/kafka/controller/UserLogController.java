package com.hadoop.kafka.controller;

import com.hadoop.kafka.common.Result;
import com.hadoop.kafka.common.ResultUtil;
import com.hadoop.kafka.model.Userlog;
import com.hadoop.kafka.producer.UserLogProducer;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "UserLogController", description = "用户日志API服务")
@RestController
@RequestMapping("/api/v1/user-log")
public class UserLogController {

    @Autowired
    private UserLogProducer userLogProducer;

    @ApiOperation("发送日志到后端")
    @RequestMapping(path = "/send", method = RequestMethod.POST)
    public Result<Object> getCarCityCountyDistribution(Userlog userlog) {
        userLogProducer.sendlog(userlog);
        return ResultUtil.success();
    }
}
