package com.hadoop.web.service;

import com.hadoop.web.entity.WastageCount;

import java.util.List;

/**
 * 流失用户数统计表
 */
public interface WastageCountService {
    List<WastageCount> findAll();
}
