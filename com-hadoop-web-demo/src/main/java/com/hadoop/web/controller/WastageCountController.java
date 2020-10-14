package com.hadoop.web.controller;

import com.hadoop.web.common.Result;
import com.hadoop.web.common.ResultUtil;
import com.hadoop.web.model.WastageCountModel;
import com.hadoop.web.service.WastageCountService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api(tags = "WastageCountController", description = "流失用户数统计表")
@RestController
@RequestMapping("/api/v1/wastageCount")
public class WastageCountController {

    @Autowired
    private WastageCountService wastageCountService;

    @ApiOperation("获取所有流失用户数")
    @RequestMapping(path = "/findAll", method = RequestMethod.GET)
    public Result<List<WastageCountModel>> findAll() {
        List<WastageCountModel> wastageCounts = wastageCountService.findAll();
        return ResultUtil.success(wastageCounts);
    }
}
