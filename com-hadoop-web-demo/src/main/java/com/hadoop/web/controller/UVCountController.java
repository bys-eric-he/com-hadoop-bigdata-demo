package com.hadoop.web.controller;

import com.hadoop.web.common.Result;
import com.hadoop.web.common.ResultUtil;
import com.hadoop.web.model.UVCountModel;
import com.hadoop.web.service.UVCountService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api(tags = "UVCountController", description = "活跃设备数统计表")
@RestController
@RequestMapping("/api/v1/uvCount")
public class UVCountController {
    @Autowired
    private UVCountService uvCountService;

    @ApiOperation("获取所有活跃设备数")
    @RequestMapping(path = "/findAll", method = RequestMethod.GET)
    public Result<List<UVCountModel>> findAll() {
        List<UVCountModel> uvCounts = uvCountService.findAll();
        return ResultUtil.success(uvCounts);
    }
}
