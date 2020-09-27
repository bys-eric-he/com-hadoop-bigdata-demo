package com.hadoop.hive.entity.log;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.sun.istack.NotNull;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.springframework.format.annotation.DateTimeFormat;

import java.time.LocalDateTime;

/**
 * 事件日志包
 */
@Data
public class EventLog {
    /**
     * 日志包Json字符串
     */
    @NotNull
    private String context;
    /**
     * 日志日期
     */
    @ApiModelProperty(value = "时间，格式：yyyy-MM-dd HH:mm:ss", hidden = false, notes = "格式：yyyy-MM-dd HH:mm:ss", dataType = "String")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern="yyyy-MM-dd HH:mm:ss")
    @DateTimeFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private LocalDateTime dateTime;
}
