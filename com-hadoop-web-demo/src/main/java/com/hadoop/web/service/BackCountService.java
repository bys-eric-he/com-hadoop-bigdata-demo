package com.hadoop.web.service;

import com.hadoop.web.entity.BackCount;

import java.util.List;

/**
 * 本周回流用户数统计
 */
public interface BackCountService {
    List<BackCount> findAll();
}
