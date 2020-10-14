package com.hadoop.web.model;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * 活跃设备数统计表
 */
@Data
@ApiModel(value = "uvCount", description = "活跃设备数统计表")
public class UVCountModel {
    /**
     * 统计日期
     */
    @ApiModelProperty(value = "统计日期", name = "dateTime", example = "2020-09-30")
    private String dateTime;
    /**
     * 当日用户数量
     */
    @ApiModelProperty(value = "当日用户数量", name = "dayCount", example = "5")
    private int dayCount;

    /**
     * 当周用户数量
     */
    @ApiModelProperty(value = "当周用户数量", name = "wkCount", example = "12")
    private int wkCount;

    /**
     * 当月用户数量
     */
    @ApiModelProperty(value = "当月用户数量", name = "mnCount", example = "23")
    private int mnCount;

    /**
     * Y,N 是否是周末,用于得到本周最终结果
     */
    @ApiModelProperty(value = "Y,N 是否是周末,用于得到本周最终结果", name = "isWeekend", example = "Y")
    private String isWeekend;

    /**
     * Y,N 是否是月末,用于得到本月最终结果
     */
    @ApiModelProperty(value = "Y,N 是否是月末,用于得到本月最终结果", name = "isMonthend", example = "Y")
    private String isMonthend;
}
