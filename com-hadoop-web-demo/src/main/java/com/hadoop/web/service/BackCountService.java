package com.hadoop.web.service;

import com.hadoop.web.model.BackCountModel;

import java.util.List;

/**
 * 本周回流用户数统计
 */
public interface BackCountService {
    List<BackCountModel> findAll();
}
