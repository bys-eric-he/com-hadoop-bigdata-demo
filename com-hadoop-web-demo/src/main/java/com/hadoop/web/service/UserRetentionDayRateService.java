package com.hadoop.web.service;

import com.hadoop.web.model.UserRetentionDayRateModel;

import java.util.List;

/**
 * 每日用户留存情况统计表
 */
public interface UserRetentionDayRateService {
    List<UserRetentionDayRateModel> findAll();
}
