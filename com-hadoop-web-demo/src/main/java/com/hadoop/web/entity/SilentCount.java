package com.hadoop.web.entity;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * 沉默用户数统计表
 */
@Data
@Entity
@Table(name = "ads_silent_count")
public class SilentCount {
    /**
     * 统计日期
     */
    @EmbeddedId
    private DateTimePrimaryKey dateTime;

    /**
     * 沉默设备数
     */
    @Column(name = "silent_count")
    private int silentCount;
}
