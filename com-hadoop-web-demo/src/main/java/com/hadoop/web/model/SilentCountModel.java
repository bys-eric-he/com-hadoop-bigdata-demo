package com.hadoop.web.model;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * 沉默用户数统计表
 */
@Data
@ApiModel(value = "silentCount", description = "沉默用户数统计表")
public class SilentCountModel {
    /**
     * 统计日期
     */
    @ApiModelProperty(value = "统计日期", name = "dateTime", example = "2020-09-30")
    private String dateTime;
    /**
     * 沉默设备数
     */
    @ApiModelProperty(value = "沉默设备数", name = "silentCount", example = "1")
    private int silentCount;
}
