package com.hadoop.web.controller;

import com.hadoop.web.common.Result;
import com.hadoop.web.common.ResultUtil;
import com.hadoop.web.entity.BackCount;
import com.hadoop.web.service.BackCountService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api(tags = "BackCountController", description = "本周回流用户数统计表")
@RestController
@RequestMapping("/api/v1/backCount")
public class BackCountController {

    @Autowired
    private BackCountService backCountService;

    @ApiOperation("获取所有回流用户数统计表")
    @RequestMapping(path = "/findAll", method = RequestMethod.GET)
    public Result<List<BackCount>> findAll() {
        List<BackCount> backCounts = backCountService.findAll();
        return ResultUtil.success(backCounts);
    }
}
