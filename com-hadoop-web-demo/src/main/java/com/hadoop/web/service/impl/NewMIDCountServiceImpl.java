package com.hadoop.web.service.impl;

import com.hadoop.web.entity.NewMIDCount;
import com.hadoop.web.repository.NewMIDCountJPARepository;
import com.hadoop.web.service.NewMIDCountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 每日新增设备信息数量统计表
 */
@Service
public class NewMIDCountServiceImpl implements NewMIDCountService {

    @Autowired
    private NewMIDCountJPARepository jpaRepository;

    @Override
    public List<NewMIDCount> findAll() {
        return jpaRepository.findAll();
    }
}
