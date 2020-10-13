package com.hadoop.web.entity;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * 本周回流用户数统计表
 */
@Data
@Entity
@Table(name = "ads_back_count")
public class BackCount {

    /**
     * 统计日期
     */
    @EmbeddedId
    private DateTimePrimaryKey dateTime;

    /**
     * 回流用户数
     */
    @Column(name = "wastage_count")
    private int wastageCount;
}
