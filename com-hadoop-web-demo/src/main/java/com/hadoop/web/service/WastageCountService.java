package com.hadoop.web.service;

import com.hadoop.web.model.WastageCountModel;

import java.util.List;

/**
 * 流失用户数统计表
 */
public interface WastageCountService {
    List<WastageCountModel> findAll();
}
