package com.hadoop.web.entity;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * 每日用户留存情况统计表
 */
@Data
@Entity
@Table(name = "ads_user_retention_day_rate")
public class UserRetentionDayRate {

    /**
     * 复合主键要用这个注解
     */
    @EmbeddedId
    private CompositePrimaryKey date;

    /**
     * 截止当前日期留存天数
     */
    @Column(name = "retention_day")
    private int retentionDay;

    /**
     * 留存数量
     */
    @Column(name = "retention_count")
    private int retentionCount;

    /**
     * 设备新增数量
     */
    @Column(name = "new_mid_count")
    private int newMidCount;

    /**
     * 留存率
     */
    @Column(name = "retention_ratio")
    private BigDecimal retentionRatio;
}
