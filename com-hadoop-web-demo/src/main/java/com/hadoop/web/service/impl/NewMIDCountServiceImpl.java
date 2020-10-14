package com.hadoop.web.service.impl;

import com.hadoop.web.entity.NewMIDCount;
import com.hadoop.web.mapping.NewMIDCountMapping;
import com.hadoop.web.model.NewMIDCountModel;
import com.hadoop.web.repository.NewMIDCountJPARepository;
import com.hadoop.web.service.NewMIDCountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * 每日新增设备信息数量统计表
 */
@Service
public class NewMIDCountServiceImpl implements NewMIDCountService {

    @Autowired
    private NewMIDCountJPARepository jpaRepository;

    @Override
    public List<NewMIDCountModel> findAll() {
        List<NewMIDCount> newMIDCounts = jpaRepository.findAll();
        List<NewMIDCountModel> results = new ArrayList<>();

        newMIDCounts.forEach(o -> results.add(NewMIDCountMapping.toModel(o)));
        return results;
    }
}
