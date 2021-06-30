package com.hadoop.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import com.hadoop.elasticsearch.component.ElasticQueryDSLTemplate;
import com.hadoop.elasticsearch.component.SpringContextHolder;
import com.hadoop.elasticsearch.response.QueryResult;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@SpringBootApplication
public class SpringBootHadoopElasticSearchApplication {
    public static void main(String[] args) {
        SpringApplication.run(SpringBootHadoopElasticSearchApplication.class, args);
        log.info("-----ElasticSearch应用服务启动完成!-----");

        ElasticQueryDSLTemplate elasticQueryDSLTemplate = SpringContextHolder.getBean(ElasticQueryDSLTemplate.class);

        /*StockMarketInfo stockMarketInfo = new StockMarketInfo();
        stockMarketInfo.setStockCode("02331");
        stockMarketInfo.setStockName("李宁");
        stockMarketInfo.setTradeDate("2021-06-30");
        stockMarketInfo.setUpAndDown("+1.63");
        stockMarketInfo.setTotalMarketValue("2415.30亿");
        Map<String, Object> docMap = new HashMap<>();
        docMap.put("stock_code", stockMarketInfo.getStockCode());
        docMap.put("stock_name", stockMarketInfo.getStockName());
        docMap.put("trade_date", stockMarketInfo.getTradeDate());
        docMap.put("up_and_down", stockMarketInfo.getUpAndDown());
        docMap.put("total_market_value", stockMarketInfo.getTotalMarketValue());
        boolean optFlag = elasticQueryDSLTemplate.addDocument("es-stock-market-index", "sm-202106301005", docMap);
        log.info(optFlag ? "-->创建索引文档成功!" : "***创建索引文档失败!");*/


        QueryResult queryResult = elasticQueryDSLTemplate.searchForPrefixQuery("es-stock-market-index", "stock_code", "02331");
        log.info("-->查询结果：" + JSONObject.toJSONString(queryResult));
        try {
            boolean optFlag = elasticQueryDSLTemplate.createIndex("es-stock-market-demo");
            log.info(optFlag ? "创建索引成功!" : "创建索引失败!");
        } catch (Exception exception) {
            log.error("****创建索引异常!****", exception);
        }
    }

    /**
     * QueryBuilder进行复合查询
     * 当使用到term 查询的时候，由于是精准匹配，所以查询的关键字在es上的类型，必须是keyword而不能是text
     *
     * @return
     */
    private static Map<String, List<QueryBuilder>> getTerms() {
        Map<String, List<QueryBuilder>> terms = new HashMap<>();
        WildcardQueryBuilder qbStockCode = QueryBuilders.wildcardQuery("stock_code", "02331");
        WildcardQueryBuilder qbTradeDate = QueryBuilders.wildcardQuery("trade_date", "2021-06-30");
        List<QueryBuilder> queryBuilders = new ArrayList<>();
        queryBuilders.add(qbStockCode);
        queryBuilders.add(qbTradeDate);
        terms.put("must", queryBuilders);

        return terms;
    }
}
