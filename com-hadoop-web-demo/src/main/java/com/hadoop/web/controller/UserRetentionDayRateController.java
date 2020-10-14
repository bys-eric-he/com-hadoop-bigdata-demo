package com.hadoop.web.controller;

import com.hadoop.web.common.Result;
import com.hadoop.web.common.ResultUtil;
import com.hadoop.web.model.UserRetentionDayRateModel;
import com.hadoop.web.service.UserRetentionDayRateService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api(tags = "UserRetentionDayRateController", description = "每日用户留存情况统计表")
@RestController
@RequestMapping("/api/v1/userRetentionDayRate")
public class UserRetentionDayRateController {

    @Autowired
    private UserRetentionDayRateService userRetentionDayRateService;

    @ApiOperation("获取所有沉默用户数")
    @RequestMapping(path = "/findAll", method = RequestMethod.GET)
    public Result<List<UserRetentionDayRateModel>> findAll() {
        List<UserRetentionDayRateModel> userRetentionDayRates = userRetentionDayRateService.findAll();
        return ResultUtil.success(userRetentionDayRates);
    }
}
