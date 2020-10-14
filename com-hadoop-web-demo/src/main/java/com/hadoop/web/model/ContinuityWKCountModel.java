package com.hadoop.web.model;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * 最近连续三周活跃用户数统计
 */
@Data
@ApiModel(value = "continuityWKCount", description = "最近连续三周活跃用户数统计")
public class ContinuityWKCountModel {
    /**
     * 统计日期
     */
    @ApiModelProperty(value = "统计日期", name = "dateTime", example = "2020-09-25")
    private String dateTime;
    /**
     * 最近 7 天日期
     */
    @ApiModelProperty(value = "最近 7 天日期", name = "weekDateTime", example = "2020-09-19_2020-09-25")
    private String weekDateTime;
    /**
     * 连续活跃设备数
     */
    @ApiModelProperty(value = "连续活跃设备数", name = "continuityCount", example = "1")
    private int continuityCount;
}
