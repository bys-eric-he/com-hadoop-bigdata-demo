package com.hadoop.web.service;

import com.hadoop.web.entity.SilentCount;

import java.util.List;

/**
 * 沉默用户数统计表
 */
public interface SilentCountService {
    List<SilentCount> findAll();
}
