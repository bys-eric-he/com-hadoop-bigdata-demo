package com.hadoop.hive.entity.user;

import lombok.Data;

/**
 * 用户访问次数
 */
@Data
public class UserAccessTimes {
    /**
     * 用户名
     */
    private String username;
    /**
     * 月份
     */
    private String month;
    /**
     * 访问次数
     */
    private int counts;
}
