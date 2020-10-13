package com.hadoop.web.service;

import com.hadoop.web.entity.UVCount;

import java.util.List;

/**
 * 活跃设备数统计表
 */
public interface UVCountService {
    List<UVCount> findAll();
}
