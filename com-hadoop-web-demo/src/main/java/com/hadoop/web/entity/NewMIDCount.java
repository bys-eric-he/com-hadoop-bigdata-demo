package com.hadoop.web.entity;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * 每日新增设备信息数量统计表
 */
@Data
@Entity
@Table(name = "ads_new_mid_count")
public class NewMIDCount {
    /**
     * 统计日期
     */
    @EmbeddedId
    private CreateDatePrimaryKey createDate;

    /**
     * 每日新增设备信息数量
     */
    @Column(name = "new_mid_count")
    private int newMidCount;
}
