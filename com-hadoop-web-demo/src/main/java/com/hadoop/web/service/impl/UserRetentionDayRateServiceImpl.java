package com.hadoop.web.service.impl;

import com.hadoop.web.entity.UserRetentionDayRate;
import com.hadoop.web.repository.UserRetentionDayRateJPARepository;
import com.hadoop.web.service.UserRetentionDayRateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 每日用户留存情况统计表
 */
@Service
public class UserRetentionDayRateServiceImpl implements UserRetentionDayRateService {

    @Autowired
    private UserRetentionDayRateJPARepository jpaRepository;

    @Override
    public List<UserRetentionDayRate> findAll() {
        return jpaRepository.findAll();
    }
}
