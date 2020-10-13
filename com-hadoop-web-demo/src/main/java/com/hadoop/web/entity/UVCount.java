package com.hadoop.web.entity;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * 活跃设备数统计表
 */
@Data
@Entity
@Table(name = "ads_uv_count")
public class UVCount {
    /**
     * 统计日期
     */
    @EmbeddedId
    private DateTimePrimaryKey dateTime;

    /**
     * 当日用户数量
     */
    @Column(name = "day_count")
    private int dayCount;

    /**
     * 当周用户数量
     */
    @Column(name = "wk_count")
    private int wkCount;

    /**
     * 当月用户数量
     */
    @Column(name = "mn_count")
    private int mnCount;

    /**
     * Y,N 是否是周末,用于得到本周最终结果
     */
    @Column(name = "is_weekend")
    private String isWeekend;

    /**
     * Y,N 是否是月末,用于得到本月最终结果
     */
    @Column(name = "is_monthend")
    private String isMonthend;
}
