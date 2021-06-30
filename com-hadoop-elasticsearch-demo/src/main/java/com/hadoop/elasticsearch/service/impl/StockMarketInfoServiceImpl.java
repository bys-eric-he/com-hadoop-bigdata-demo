package com.hadoop.elasticsearch.service.impl;

import com.hadoop.elasticsearch.component.ElasticQueryDSLTemplate;
import com.hadoop.elasticsearch.service.StockMarketInfoService;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class StockMarketInfoServiceImpl implements StockMarketInfoService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Autowired
    private ElasticQueryDSLTemplate elasticQueryDSLTemplate;
}
