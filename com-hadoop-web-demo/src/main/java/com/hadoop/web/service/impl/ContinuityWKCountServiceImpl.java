package com.hadoop.web.service.impl;

import com.hadoop.web.entity.ContinuityWKCount;
import com.hadoop.web.mapping.ContinuityWKCountMapping;
import com.hadoop.web.model.ContinuityWKCountModel;
import com.hadoop.web.repository.ContinuityWKCountJPARepository;
import com.hadoop.web.service.ContinuityWKCountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * 最近连续三周活跃用户数统计
 */
@Service
public class ContinuityWKCountServiceImpl implements ContinuityWKCountService {

    @Autowired
    private ContinuityWKCountJPARepository jpaRepository;

    @Override
    public List<ContinuityWKCountModel> findAll() {

        List<ContinuityWKCount> continuityWKCounts = jpaRepository.findAll();

        List<ContinuityWKCountModel> results = new ArrayList<>();
        continuityWKCounts.forEach(o -> results.add(ContinuityWKCountMapping.toModel(o)));

        return results;
    }
}
