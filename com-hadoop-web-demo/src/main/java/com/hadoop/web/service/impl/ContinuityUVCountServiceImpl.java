package com.hadoop.web.service.impl;

import com.hadoop.web.entity.ContinuityUVCount;
import com.hadoop.web.repository.ContinuityUVCountJPARepository;
import com.hadoop.web.service.ContinuityUVCountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 连续活跃设备数
 */
@Service
public class ContinuityUVCountServiceImpl implements ContinuityUVCountService {

    @Autowired
    private ContinuityUVCountJPARepository jpaRepository;

    @Override
    public List<ContinuityUVCount> findAll() {
        return jpaRepository.findAll();
    }
}
