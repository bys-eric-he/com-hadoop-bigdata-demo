package com.hadoop.web.entity;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * 最近连续三周活跃用户数统计
 */
@Data
@Entity
@Table(name = "ads_continuity_wk_count")
public class ContinuityWKCount {
    /**
     * 统计日期
     */
    @EmbeddedId
    private DateTimePrimaryKey dateTime;

    /**
     * 最近 7 天日期
     */
    @Column(name = "wk_dt", nullable = true)
    private String weekDateTime;

    /**
     * 连续活跃设备数
     */
    @Column(name = "continuity_count", nullable = true)
    private int continuityCount;
}
