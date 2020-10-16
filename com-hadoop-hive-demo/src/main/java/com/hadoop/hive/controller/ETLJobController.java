package com.hadoop.hive.controller;

import com.hadoop.hive.common.DateUtil;
import com.hadoop.hive.common.Result;
import com.hadoop.hive.common.ResultUtil;
import com.hadoop.hive.constant.HiveSQL;
import com.hadoop.hive.service.ads.StartLogADSService;
import com.hadoop.hive.service.dwd.EventLogDWDService;
import com.hadoop.hive.service.dwd.StartLogDWDService;
import com.hadoop.hive.service.dws.StartLogDWSService;
import com.hadoop.hive.service.dwt.StartLogDWTService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

@Api(tags = "ETLJobController", description = "ETL数据清洗、抽取、加载控制器")
@RestController
@RequestMapping("/api/v1/etl")
public class ETLJobController {

    @Autowired
    @Qualifier("startLogDWDService")
    private StartLogDWDService startLogDWDService;

    @Autowired
    @Qualifier("eventLogDWDService")
    private EventLogDWDService eventLogDWDService;

    @Autowired
    @Qualifier("startLogDWSService")
    private StartLogDWSService startLogDWSService;

    @Autowired
    @Qualifier("startLogDWTService")
    private StartLogDWTService startLogDWTService;

    @Autowired
    @Qualifier("startLogADSService")
    private StartLogADSService startLogADSService;

    @ApiOperation("App操作行为日志 DWD明细数据层操作，对ODS层进行数据清洗")
    @RequestMapping(path = "/dwd/eventLog", method = RequestMethod.GET)
    public Result<Object> jobEventLogODSToDWDExecute(
            @RequestParam(value = "dateTime", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDateTime dateTime) {
        try {
            if (dateTime == null) {
                dateTime = DateUtil.getCurrentDateTime();
            }
            String sql = String.format(
                    HiveSQL.SQL_ODS_TO_DWD_EVENT,
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"));

            eventLogDWDService.execute(sql);
            return ResultUtil.success();
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.failureDefaultError();
        }
    }

    @ApiOperation("App操作行为日志 DWD明细数据层操作，抽取评论数据")
    @RequestMapping(path = "/dwd/eventLog/comment", method = RequestMethod.GET)
    public Result<Object> jobEventLogODSToDWDCommentExecute(
            @RequestParam(value = "dateTime", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDateTime dateTime) {
        try {
            if (dateTime == null) {
                dateTime = DateUtil.getCurrentDateTime();
            }
            String sql = String.format(
                    HiveSQL.SQL_ODS_TO_DWD_COMMENT_EVENT,
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"));

            eventLogDWDService.comment(sql);
            return ResultUtil.success();
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.failureDefaultError();
        }
    }

    @ApiOperation("App操作行为日志 DWD明细数据层操作，抽取点赞数据")
    @RequestMapping(path = "/dwd/eventLog/praise", method = RequestMethod.GET)
    public Result<Object> jobEventLogODSToDWDPraiseExecute(
            @RequestParam(value = "dateTime", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDateTime dateTime) {
        try {
            if (dateTime == null) {
                dateTime = DateUtil.getCurrentDateTime();
            }
            String sql = String.format(
                    HiveSQL.SQL_ODS_TO_DWD_PRAISE_EVENT,
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"));

            eventLogDWDService.praise(sql);
            return ResultUtil.success();
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.failureDefaultError();
        }
    }

    @ApiOperation("App操作行为日志 DWD明细数据层操作，抽取活跃用户数据")
    @RequestMapping(path = "/dwd/eventLog/active", method = RequestMethod.GET)
    public Result<Object> jobEventLogODSToDWDActiveExecute(
            @RequestParam(value = "dateTime", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDateTime dateTime) {
        try {
            if (dateTime == null) {
                dateTime = DateUtil.getCurrentDateTime();
            }
            String sql = String.format(
                    HiveSQL.SQL_ODS_TO_DWD_ACTIVE_EVENT,
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"));

            eventLogDWDService.active(sql);
            return ResultUtil.success();
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.failureDefaultError();
        }
    }

    @ApiOperation("App启动日志 DWD明细数据层操作，对ODS层进行数据清洗")
    @RequestMapping(path = "/dwd/startLog", method = RequestMethod.GET)
    public Result<Object> jobStartLogODSToDWDExecute(
            @RequestParam(value = "dateTime", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDateTime dateTime) {
        try {
            if (dateTime == null) {
                dateTime = DateUtil.getCurrentDateTime();
            }
            String sql = String.format(
                    HiveSQL.SQL_ODS_TO_DWD_START,
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"));

            startLogDWDService.execute(sql);

            return ResultUtil.success();
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.failureDefaultError();
        }
    }

    @ApiOperation("App启动日志 DWS服务数据层，以DWD为基础按天进行轻度汇总")
    @RequestMapping(path = "/dws/startLog", method = RequestMethod.GET)
    public Result<Object> jobStartLogDWDToDWSExecute(
            @RequestParam(value = "dateTime", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDateTime dateTime) {
        try {
            if (dateTime == null) {
                dateTime = DateUtil.getCurrentDateTime();
            }
            String sql = String.format(
                    HiveSQL.SQL_DWD_TO_DWS_START,
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"));

            startLogDWSService.execute(sql);
            return ResultUtil.success();
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.failureDefaultError();
        }
    }

    @ApiOperation("App启动日志 DWT数据主题层，以DWS层为基础按主题进行汇总")
    @RequestMapping(path = "/dwt/startLog", method = RequestMethod.GET)
    public Result<Object> jobStartLogDWSToDWTExecute(
            @RequestParam(value = "dateTime", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDateTime dateTime) {
        try {
            if (dateTime == null) {
                dateTime = DateUtil.getCurrentDateTime();
            }
            String sql = String.format(
                    HiveSQL.SQL_DWS_TO_DWT_START,
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"));

            startLogDWTService.execute(sql);
            return ResultUtil.success();
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.failureDefaultError();
        }
    }

    @ApiOperation("根据DWT数据主题层 生成ADS应用层数据-统计活跃设备数")
    @RequestMapping(path = "/ads/activeDevices", method = RequestMethod.GET)
    public Result<Object> activeDevices(
            @RequestParam(value = "dateTime", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDateTime dateTime) {
        try {
            if (dateTime == null) {
                dateTime = DateUtil.getCurrentDateTime();
            }
            String sql = String.format(
                    HiveSQL.SQL_DWT_TO_ADS_ACTIVE_DEVICES_START,
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"));

            startLogADSService.activeDevices(sql);
            return ResultUtil.success();
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.failureDefaultError();
        }
    }

    @ApiOperation("根据DWT数据主题层 生成ADS应用层数据-统计连续活跃设备数")
    @RequestMapping(path = "/ads/continuousActiveDevices", method = RequestMethod.GET)
    public Result<Object> continuousActiveDevices(
            @RequestParam(value = "dateTime", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDateTime dateTime) {
        try {
            if (dateTime == null) {
                dateTime = DateUtil.getCurrentDateTime();
            }
            String sql = String.format(
                    HiveSQL.SQL_DWT_TO_ADS_CONTINUOUS_ACTIVE_DEVICES_START,
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"));

            startLogADSService.continuousActiveDevices(sql);
            return ResultUtil.success();
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.failureDefaultError();
        }
    }

    @ApiOperation("根据DWT数据主题层 生成ADS应用层数据-统计最近连续三周活跃用户数")
    @RequestMapping(path = "/ads/threeConsecutiveWeeks", method = RequestMethod.GET)
    public Result<Object> threeConsecutiveWeeks(
            @RequestParam(value = "dateTime", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDateTime dateTime) {
        try {
            if (dateTime == null) {
                dateTime = DateUtil.getCurrentDateTime();
            }
            String sql = String.format(
                    HiveSQL.SQL_DWT_TO_ADS_THREE_CONSECUTIVE_WEEKS_START,
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"));

            startLogADSService.threeConsecutiveWeeks(sql);
            return ResultUtil.success();
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.failureDefaultError();
        }
    }

    @ApiOperation("根据DWT数据主题层 生成ADS应用层数据-统计每日用户留存情况")
    @RequestMapping(path = "/ads/dailyUserRetentionStatus", method = RequestMethod.GET)
    public Result<Object> dailyUserRetentionStatus(
            @RequestParam(value = "dateTime", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDateTime dateTime) {
        try {
            if (dateTime == null) {
                dateTime = DateUtil.getCurrentDateTime();
            }
            String sql = String.format(
                    HiveSQL.SQL_DWT_TO_ADS_DAILY_USER_RETENTION_STATUS_START,
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"));

            startLogADSService.dailyUserRetentionStatus(sql);
            return ResultUtil.success();
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.failureDefaultError();
        }
    }

    @ApiOperation("根据DWT数据主题层 生成ADS应用层数据-统计流失用户数")
    @RequestMapping(path = "/ads/lostUsers", method = RequestMethod.GET)
    public Result<Object> lostUsers(
            @RequestParam(value = "dateTime", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDateTime dateTime) {
        try {
            if (dateTime == null) {
                dateTime = DateUtil.getCurrentDateTime();
            }
            String sql = String.format(
                    HiveSQL.SQL_DWT_TO_ADS_LOST_USERS_START,
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"));

            startLogADSService.lostUsers(sql);
            return ResultUtil.success();
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.failureDefaultError();
        }
    }

    @ApiOperation("根据DWT数据主题层 生成ADS应用层数据-统计每日新增设备信息数量")
    @RequestMapping(path = "/ads/newDeviceAddedDaily", method = RequestMethod.GET)
    public Result<Object> newDeviceAddedDaily(
            @RequestParam(value = "dateTime", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDateTime dateTime) {
        try {
            if (dateTime == null) {
                dateTime = DateUtil.getCurrentDateTime();
            }
            String sql = String.format(
                    HiveSQL.SQL_DWT_TO_ADS_NUMBER_OF_NEW_DEVICE_ADDED_DAILY_START,
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"));

            startLogADSService.newDeviceAddedDaily(sql);
            return ResultUtil.success();
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.failureDefaultError();
        }
    }

    @ApiOperation("根据DWT数据主题层 生成ADS应用层数据-统计沉默用户数")
    @RequestMapping(path = "/ads/numberOfSilentUsers", method = RequestMethod.GET)
    public Result<Object> numberOfSilentUsers(
            @RequestParam(value = "dateTime", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDateTime dateTime) {
        try {
            if (dateTime == null) {
                dateTime = DateUtil.getCurrentDateTime();
            }
            String sql = String.format(
                    HiveSQL.SQL_DWT_TO_ADS_NUMBER_OF_SILENT_USERS_START,
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"));

            startLogADSService.numberOfSilentUsers(sql);
            return ResultUtil.success();
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.failureDefaultError();
        }
    }

    @ApiOperation("根据DWT数据主题层 生成ADS应用层数据-统计本周回流用户数")
    @RequestMapping(path = "/ads/returningUsers", method = RequestMethod.GET)
    public Result<Object> returningUsers(
            @RequestParam(value = "dateTime", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDateTime dateTime) {
        try {
            if (dateTime == null) {
                dateTime = DateUtil.getCurrentDateTime();
            }
            String sql = String.format(
                    HiveSQL.SQL_DWT_TO_ADS_RETURNING_USERS_START,
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"),
                    DateUtil.getLocalDateTime(dateTime, "yyyy-MM-dd"));

            startLogADSService.returningUsers(sql);
            return ResultUtil.success();
        } catch (Exception e) {
            e.printStackTrace();
            return ResultUtil.failureDefaultError();
        }
    }
}
