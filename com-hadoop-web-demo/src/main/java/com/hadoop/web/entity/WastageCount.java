package com.hadoop.web.entity;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * 流失用户数统计表
 */
@Data
@Entity
@Table(name = "ads_wastage_count")
public class WastageCount {
    /**
     * 统计日期
     */
    @EmbeddedId
    private DateTimePrimaryKey dateTime;

    /**
     * 流失设备数
     */
    @Column(name = "wastage_count", nullable = false)
    private int wastageCount;
}
