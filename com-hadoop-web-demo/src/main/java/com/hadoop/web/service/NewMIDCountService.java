package com.hadoop.web.service;

import com.hadoop.web.entity.NewMIDCount;

import java.util.List;

/**
 * 每日新增设备信息数量统计表
 */
public interface NewMIDCountService {
    List<NewMIDCount> findAll();
}
