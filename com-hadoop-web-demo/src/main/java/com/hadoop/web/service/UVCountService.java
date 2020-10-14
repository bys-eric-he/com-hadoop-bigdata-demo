package com.hadoop.web.service;

import com.hadoop.web.model.UVCountModel;

import java.util.List;

/**
 * 活跃设备数统计表
 */
public interface UVCountService {
    List<UVCountModel> findAll();
}
