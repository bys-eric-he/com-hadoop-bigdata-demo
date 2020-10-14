package com.hadoop.web.service;

import com.hadoop.web.model.SilentCountModel;

import java.util.List;

/**
 * 沉默用户数统计表
 */
public interface SilentCountService {
    List<SilentCountModel> findAll();
}
