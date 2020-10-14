package com.hadoop.web.service.impl;

import com.hadoop.web.entity.UVCount;
import com.hadoop.web.mapping.UVCountMapping;
import com.hadoop.web.model.UVCountModel;
import com.hadoop.web.repository.UVCountJPARepository;
import com.hadoop.web.service.UVCountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * 活跃设备数统计表
 */
@Service
public class UVCountServiceImpl implements UVCountService {

    @Autowired
    private UVCountJPARepository jpaRepository;

    @Override
    public List<UVCountModel> findAll() {
        List<UVCount> uvCounts = jpaRepository.findAll();
        List<UVCountModel> results = new ArrayList<>();
        uvCounts.forEach(o -> results.add(UVCountMapping.toModel(o)));
        return results;
    }
}
