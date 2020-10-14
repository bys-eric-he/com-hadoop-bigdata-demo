package com.hadoop.web.model;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * 每日新增设备信息数量统计表
 */
@Data
@ApiModel(value = "newMIDCount", description = "每日新增设备信息数量统计表")
public class NewMIDCountModel {
    /**
     * 统计日期
     */
    @ApiModelProperty(value = "统计日期", name = "createDate", example = "2020-09-25")
    private String createDate;
    /**
     * 每日新增设备信息数量
     */
    @ApiModelProperty(value = "每日新增设备信息数量", name = "newMidCount", example = "3")
    private int newMidCount;
}
