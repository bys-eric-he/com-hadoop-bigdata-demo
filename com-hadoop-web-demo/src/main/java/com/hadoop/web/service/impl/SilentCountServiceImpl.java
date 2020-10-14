package com.hadoop.web.service.impl;

import com.hadoop.web.entity.SilentCount;
import com.hadoop.web.mapping.SilentCountMapping;
import com.hadoop.web.model.SilentCountModel;
import com.hadoop.web.repository.SilentCountJPARepository;
import com.hadoop.web.service.SilentCountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * 沉默用户数统计表
 */
@Service
public class SilentCountServiceImpl implements SilentCountService {

    @Autowired
    private SilentCountJPARepository jpaRepository;

    @Override
    public List<SilentCountModel> findAll() {
        List<SilentCount> silentCounts = jpaRepository.findAll();
        List<SilentCountModel> results = new ArrayList<>();
        silentCounts.forEach(o -> results.add(SilentCountMapping.toModel(o)));
        return results;
    }
}
