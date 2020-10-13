package com.hadoop.web.service;

import com.hadoop.web.entity.ContinuityWKCount;

import java.util.List;

/**
 * 最近连续三周活跃用户数统计
 */
public interface ContinuityWKCountService {
    List<ContinuityWKCount> findAll();
}
