package com.hadoop.web.service.impl;

import com.hadoop.web.entity.WastageCount;
import com.hadoop.web.mapping.WastageCountMapping;
import com.hadoop.web.model.WastageCountModel;
import com.hadoop.web.repository.WastageCountJPARepository;
import com.hadoop.web.service.WastageCountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * 流失用户数统计表
 */
@Service
public class WastageCountServiceImpl implements WastageCountService {

    @Autowired
    private WastageCountJPARepository jpaRepository;

    @Override
    public List<WastageCountModel> findAll() {
        List<WastageCount> wastageCounts = jpaRepository.findAll();

        List<WastageCountModel> results = new ArrayList<>();
        wastageCounts.forEach(o -> results.add(WastageCountMapping.toModel(o)));

        return results;
    }
}
