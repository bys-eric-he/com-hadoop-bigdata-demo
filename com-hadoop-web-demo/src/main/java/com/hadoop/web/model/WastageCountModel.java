package com.hadoop.web.model;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * 流失用户数统计表
 */
@Data
@ApiModel(value = "wastageCount", description = "流失用户数统计表")
public class WastageCountModel {
    /**
     * 统计日期
     */
    @ApiModelProperty(value = "统计日期", name = "dateTime", example = "2020-09-30")
    private String dateTime;
    /**
     * 流失设备数
     */
    @ApiModelProperty(value = "流失设备数", name = "wastageCount", example = "5")
    private int wastageCount;
}
