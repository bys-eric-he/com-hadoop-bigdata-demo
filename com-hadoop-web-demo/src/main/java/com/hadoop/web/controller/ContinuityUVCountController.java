package com.hadoop.web.controller;

import com.hadoop.web.common.Result;
import com.hadoop.web.common.ResultUtil;
import com.hadoop.web.model.ContinuityUVCountModel;
import com.hadoop.web.service.ContinuityUVCountService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api(tags = "ContinuityUVCountController", description = "连续活跃设备数")
@RestController
@RequestMapping("/api/v1/continuityUVCount")
public class ContinuityUVCountController {

    @Autowired
    private ContinuityUVCountService continuityUVCountService;

    @ApiOperation("获取所有连续活跃设备数")
    @RequestMapping(path = "/findAll", method = RequestMethod.GET)
    public Result<List<ContinuityUVCountModel>> findAll() {
        List<ContinuityUVCountModel> continuityUVCounts = continuityUVCountService.findAll();
        return ResultUtil.success(continuityUVCounts);
    }
}
