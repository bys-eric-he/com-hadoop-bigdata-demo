package com.hadoop.web.model;


import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.math.BigDecimal;

/**
 * 每日用户留存情况统计表
 */
@Data
@ApiModel(value = "silentCount", description = "沉默用户数统计表")
public class UserRetentionDayRateModel {
    /**
     * 统计日期
     */
    @ApiModelProperty(value = "统计日期", name = "dateTime", example = "2020-09-30")
    private String dateTime;

    /**
     * 设备新增日期
     */
    @ApiModelProperty(value = "设备新增日期", name = "createDate", example = "2020-09-30")
    private String createDate;

    /**
     * 截止当前日期留存天数
     */
    @ApiModelProperty(value = "截止当前日期留存天数", name = "retentionDay", example = "1")
    private int retentionDay;

    /**
     * 留存数量
     */
    @ApiModelProperty(value = "留存数量", name = "retentionCount", example = "1")
    private int retentionCount;

    /**
     * 设备新增数量
     */
    @ApiModelProperty(value = "设备新增数量", name = "newMidCount", example = "1")
    private int newMidCount;

    /**
     * 留存率
     */
    @ApiModelProperty(value = "留存率", name = "retentionRatio", example = "1.2")
    private BigDecimal retentionRatio;
}
