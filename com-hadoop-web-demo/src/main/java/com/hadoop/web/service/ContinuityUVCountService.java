package com.hadoop.web.service;

import com.hadoop.web.entity.ContinuityUVCount;

import java.util.List;

/**
 * 连续活跃设备数
 */
public interface ContinuityUVCountService {
    List<ContinuityUVCount> findAll();
}
