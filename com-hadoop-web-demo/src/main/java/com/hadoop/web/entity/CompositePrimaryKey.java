package com.hadoop.web.entity;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

/**
 * 复合主键类
 */
@Data
@Embeddable
public class CompositePrimaryKey implements Serializable {
    /**
     * 统计日期
     */
    @Column(name = "stat_date", nullable = false)
    private String dateTime;

    /**
     * 设备新增日期
     */
    @Column(name = "create_date", nullable = false)
    private String createDate;
}
