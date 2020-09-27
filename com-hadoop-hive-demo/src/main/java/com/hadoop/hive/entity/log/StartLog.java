package com.hadoop.hive.entity.log;


import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.springframework.format.annotation.DateTimeFormat;

import java.time.LocalDateTime;

/**
 * 启动日志数据包
 */
@Data
public class StartLog {
    /**
     * 日志包Json字符串
     */
    private String context;
    /**
     * 日志日期
     */
    @ApiModelProperty(value = "时间，格式：yyyy-MM-dd HH:mm:ss", hidden = false, notes = "格式：yyyy-MM-dd HH:mm:ss", dataType = "String")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime dateTime;
}
