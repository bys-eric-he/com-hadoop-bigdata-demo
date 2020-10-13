package com.hadoop.web.service.impl;

import com.hadoop.web.entity.WastageCount;
import com.hadoop.web.repository.WastageCountJPARepository;
import com.hadoop.web.service.WastageCountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 流失用户数统计表
 */
@Service
public class WastageCountServiceImpl implements WastageCountService {

    @Autowired
    private WastageCountJPARepository jpaRepository;

    @Override
    public List<WastageCount> findAll() {
        return jpaRepository.findAll();
    }
}
