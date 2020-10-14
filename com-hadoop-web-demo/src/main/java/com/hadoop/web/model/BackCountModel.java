package com.hadoop.web.model;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * 本周回流用户数统计表
 */
@Data
@ApiModel(value = "backCount", description = "本周回流用户数统计表")
public class BackCountModel {
    /**
     * 统计日期
     */
    @ApiModelProperty(value = "统计日期", name = "dateTime", example = "2020-10-10")
    private String dateTime;
    /**
     * 回流用户数
     */
    @ApiModelProperty(value = "回流用户数", name = "wastageCount", example = "3")
    private int wastageCount;
}
