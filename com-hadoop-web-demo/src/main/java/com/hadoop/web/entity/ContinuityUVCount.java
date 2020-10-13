package com.hadoop.web.entity;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * 连续活跃设备数
 */
@Data
@Entity
@Table(name = "ads_continuity_uv_count")
public class ContinuityUVCount {
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
