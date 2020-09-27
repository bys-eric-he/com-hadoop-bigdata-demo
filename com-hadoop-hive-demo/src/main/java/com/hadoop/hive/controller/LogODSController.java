package com.hadoop.hive.controller;

import com.hadoop.hive.common.Result;
import com.hadoop.hive.common.ResultUtil;
import com.hadoop.hive.entity.log.EventLog;
import com.hadoop.hive.entity.log.StartLog;
import com.hadoop.hive.service.ods.ODSService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "LogController", description = "日志ODS原始数据采集控制器")
@RestController
@RequestMapping("/api/v1/start-log")
public class LogODSController {

    @Autowired
    private ODSService<StartLog> startLogODSService;

    @Autowired
    private ODSService<EventLog> eventLogODSService;

    @ApiOperation("存储启动日志ODS层原始数据")
    @RequestMapping(path = "/insert/start", method = RequestMethod.POST)
    public Result<Object> insertStart(@Validated StartLog startLog) {
        startLogODSService.insert(startLog);
        return ResultUtil.success();
    }

    @ApiOperation("存储行为日志ODS层原始数据")
    @RequestMapping(path = "/insert/event", method = RequestMethod.POST)
    public Result<Object> insertEvent(@Validated EventLog eventLog) {
        eventLogODSService.insert(eventLog);
        return ResultUtil.success();
    }
}
