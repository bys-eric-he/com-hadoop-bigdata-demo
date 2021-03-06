package com.hadoop.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 用户行为操作
 */
@Data
@AllArgsConstructor
public class UserAction {
    /**
     * 用户ID
     */
    private String userID;
    /**
     * 发生时间
     */
    private Long eventTime;
    /**
     * 事件类型
     */
    private String eventType;
    /**
     * 产品ID
     */
    private String productID;
    /**
     * 产品价格
     */
    private Double productPrice;
}
