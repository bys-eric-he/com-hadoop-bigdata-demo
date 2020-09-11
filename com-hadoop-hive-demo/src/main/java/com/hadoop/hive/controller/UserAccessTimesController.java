package com.hadoop.hive.controller;

import com.hadoop.hive.common.Result;
import com.hadoop.hive.common.ResultUtil;
import com.hadoop.hive.entity.user.UserAccessTimes;
import com.hadoop.hive.service.UserAccessTimesService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api(tags = "UserAccessTimesController", description = "用户访问次数统计")
@RestController
@RequestMapping("/api/v1/userAccess")
public class UserAccessTimesController {

    @Autowired
    private UserAccessTimesService service;

    @ApiOperation("获取用户访问次数原始数据")
    @RequestMapping(path = "/getUserAccessTimesList", method = RequestMethod.GET)
    public Result<List<UserAccessTimes>> getUserAccessTimesList() {
        List<UserAccessTimes> result = service.getUserAccessTimesList();
        return ResultUtil.success(result);
    }

    @ApiOperation("按用户和月份分组统计访问次数")
    @RequestMapping(path = "/getUserAccessTimeGroupByNameAndMonth", method = RequestMethod.GET)
    public Result<List<UserAccessTimes>> getUserAccessTimeGroupByNameAndMonth() {
        List<UserAccessTimes> result = service.getUserAccessTimeGroupByNameAndMonth();
        return ResultUtil.success(result);
    }
}
