package com.hadoop.web.controller;

import com.hadoop.web.common.Result;
import com.hadoop.web.common.ResultUtil;
import com.hadoop.web.entity.NewMIDCount;
import com.hadoop.web.service.NewMIDCountService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api(tags = "NewMIDCountController", description = "每日新增设备信息数量统计表")
@RestController
@RequestMapping("/api/v1/newMIDCount")
public class NewMIDCountController {

    @Autowired
    private NewMIDCountService newMIDCountService;

    @ApiOperation("获取所有最近连续三周活跃用户数")
    @RequestMapping(path = "/findAll", method = RequestMethod.GET)
    public Result<List<NewMIDCount>> findAll() {
        List<NewMIDCount> newMIDCounts = newMIDCountService.findAll();
        return ResultUtil.success(newMIDCounts);
    }
}
