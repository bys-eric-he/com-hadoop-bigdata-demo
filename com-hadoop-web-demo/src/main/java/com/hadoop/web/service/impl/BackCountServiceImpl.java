package com.hadoop.web.service.impl;

import com.hadoop.web.entity.BackCount;
import com.hadoop.web.repository.BackCountJPARepository;
import com.hadoop.web.service.BackCountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 本周回流用户数统计
 */
@Service
public class BackCountServiceImpl implements BackCountService {

    @Autowired
    private BackCountJPARepository jpaRepository;

    @Override
    public List<BackCount> findAll() {
        return jpaRepository.findAll();
    }
}
