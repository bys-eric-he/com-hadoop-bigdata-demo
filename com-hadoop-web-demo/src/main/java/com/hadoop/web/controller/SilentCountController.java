package com.hadoop.web.controller;

import com.hadoop.web.common.Result;
import com.hadoop.web.common.ResultUtil;
import com.hadoop.web.model.SilentCountModel;
import com.hadoop.web.service.SilentCountService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api(tags = "SilentCountController", description = "沉默用户数统计表")
@RestController
@RequestMapping("/api/v1/silentCount")
public class SilentCountController {

    @Autowired
    private SilentCountService silentCountService;

    @ApiOperation("获取所有沉默用户数")
    @RequestMapping(path = "/findAll", method = RequestMethod.GET)
    public Result<List<SilentCountModel>> findAll() {
        List<SilentCountModel> silentCounts = silentCountService.findAll();
        return ResultUtil.success(silentCounts);
    }
}
