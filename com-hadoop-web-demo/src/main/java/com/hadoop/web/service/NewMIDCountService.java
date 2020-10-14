package com.hadoop.web.service;

import com.hadoop.web.model.NewMIDCountModel;

import java.util.List;

/**
 * 每日新增设备信息数量统计表
 */
public interface NewMIDCountService {
    List<NewMIDCountModel> findAll();
}
