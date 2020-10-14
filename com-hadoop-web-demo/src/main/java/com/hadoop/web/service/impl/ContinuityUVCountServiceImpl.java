package com.hadoop.web.service.impl;

import com.hadoop.web.entity.ContinuityUVCount;
import com.hadoop.web.mapping.ContinuityUVCountMapping;
import com.hadoop.web.model.ContinuityUVCountModel;
import com.hadoop.web.repository.ContinuityUVCountJPARepository;
import com.hadoop.web.service.ContinuityUVCountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * 连续活跃设备数
 */
@Service
public class ContinuityUVCountServiceImpl implements ContinuityUVCountService {

    @Autowired
    private ContinuityUVCountJPARepository jpaRepository;

    @Override
    public List<ContinuityUVCountModel> findAll() {
        List<ContinuityUVCount> continuityUVCounts = jpaRepository.findAll();

        List<ContinuityUVCountModel> results = new ArrayList<>();
        continuityUVCounts.forEach(o -> results.add(ContinuityUVCountMapping.toModel(o)));
        return results;
    }
}
