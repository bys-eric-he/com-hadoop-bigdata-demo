package com.hadoop.kafka.model;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * 用户日志信息
 */
@Data
@Accessors
public class UserLog implements Serializable {
    private static final long serialVersionUID = 5071239632319759222L;
    private String username;
    private String userId;
    private String state;
    private String content;
}