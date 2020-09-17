package com.hadoop.kafka.model;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * 用户日志信息
 */
@Data
@Accessors
public class Userlog {
    private String username;
    private String userid;
    private String state;
    private String content;
}