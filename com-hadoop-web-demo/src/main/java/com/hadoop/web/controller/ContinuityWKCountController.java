package com.hadoop.web.controller;

import com.hadoop.web.common.Result;
import com.hadoop.web.common.ResultUtil;
import com.hadoop.web.model.ContinuityWKCountModel;
import com.hadoop.web.service.ContinuityWKCountService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api(tags = "ContinuityWKCountController", description = "最近连续三周活跃用户数统计")
@RestController
@RequestMapping("/api/v1/continuityWKCount")
public class ContinuityWKCountController {
    @Autowired
    private ContinuityWKCountService continuityWKCountService;

    @ApiOperation("获取所有最近连续三周活跃用户数")
    @RequestMapping(path = "/findAll", method = RequestMethod.GET)
    public Result<List<ContinuityWKCountModel>> findAll() {
        List<ContinuityWKCountModel> continuityWKCounts = continuityWKCountService.findAll();
        return ResultUtil.success(continuityWKCounts);
    }
}
