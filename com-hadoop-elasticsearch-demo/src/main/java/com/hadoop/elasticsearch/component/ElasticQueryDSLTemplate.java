package com.hadoop.elasticsearch.component;

import com.alibaba.fastjson.JSONObject;
import com.hadoop.elasticsearch.response.QueryResult;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * ElasticQueryDSLTemplates模板类【基于Query DSL】
 *
 * @author He.Yong
 * @since 2021-06-29 17:18:28
 */
@Component
public class ElasticQueryDSLTemplate {

    private static final Logger log = LoggerFactory.getLogger(ElasticQueryDSLTemplate.class);

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * 创建索引 该方法需要手动下载安装同版本的ik分词器
     * <p>
     * 否则会报：Elasticsearch exception [type=mapper_parsing_exception, reason=Failed to parse mapping [_doc]: analyzer [ik_smart] not found for field [fullText]]异常
     *
     * @param indexName
     * @return
     * @throws IOException
     */
    public boolean createIndex(String indexName) throws IOException {
        Assert.notNull(indexName, "索引名称不能为空");
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3) // 分片
                .put("index.number_of_replicas", 1) //副本
                .put("refresh_interval", "10s")
        );

        // 创建fullText属性
        Map<String, Object> fullText = new HashMap<>();
        fullText.put("type", "text");
        fullText.put("analyzer", "ik_max_word"); // 可选ik_max_word、ik_smart
        fullText.put("search_analyzer", "ik_smart"); // 可选ik_max_word、ik_smart
        fullText.put("term_vector", "with_positions_offsets"); // 全文检索fvh设置

        // 创建fondCode属性
        Map<String, Object> fondCode = new HashMap<>();
        fondCode.put("type", "keyword");

        Map<String, Object> properties = new HashMap<>();
        properties.put("fullText", fullText);
        properties.put("fondCode", fondCode);

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);
        request.mapping(mapping);

        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        return createIndexResponse.isAcknowledged();
    }

    /**
     * 删除索引
     *
     * @param indexName 索引名称
     * @return
     * @throws IOException
     */
    public boolean deleteIndex(String indexName) throws IOException {
        Assert.notNull(indexName, "索引名称不能为空");
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        AcknowledgedResponse deleteIndexResponse = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        return deleteIndexResponse.isAcknowledged();
    }

    /**
     * 判断索引是否存在
     *
     * @param indexName 索引名称
     * @return
     * @throws IOException
     */
    public boolean existsIndex(String indexName) throws IOException {
        Assert.notNull(indexName, "索引名称不能为空");
        GetIndexRequest request = new GetIndexRequest(indexName);
        return restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
    }

    /**
     * 添加索引文档
     *
     * @param indexName 索引名称
     * @param id        文档id
     * @param docMap    文档map
     * @return
     * @throws IOException
     */
    public boolean addDocument(String indexName, String id, Map<String, Object> docMap) {
        boolean optFlag = Boolean.TRUE;
        try {
            Assert.notNull(indexName, "索引名称不能为空");
            Assert.notNull(id, "索引文档ID不能为空");
            Assert.notNull(docMap, "索引文档docMap不能为空");
            IndexRequest indexRequest = new IndexRequest(indexName).id(id).source(docMap);
            indexRequest.opType(DocWriteRequest.OpType.CREATE);
            restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("添加索引文档失败异常。索引名称【{}】,索引文档ID【{}】", indexName, id, e);
            optFlag = Boolean.FALSE;
        }
        return optFlag;
    }

    /**
     * 根据ID获取索引文档
     *
     * @param indexName 索引名称
     * @param id        文档id
     * @return
     * @throws IOException
     */
    public Map<String, Object> getDocument(String indexName, String id) {
        Map<String, Object> docMap = null;
        try {
            Assert.notNull(indexName, "索引名称不能为空");
            Assert.notNull(id, "索引文档ID不能为空");
            GetRequest request = new GetRequest(indexName, id);
            GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);
            docMap = response.getSourceAsMap();
        } catch (Exception e) {
            log.error("根据ID获取索引文档异常。索引名称【{}】,索引文档ID【{}】", indexName, id, e);
        }
        return docMap;
    }

    /**
     * 根据ID判断索引文档是否存在
     *
     * @param indexName 索引名称
     * @param id        文档id
     * @return
     * @throws IOException
     */
    public boolean existsDocument(String indexName, String id) {
        boolean optFlag = Boolean.TRUE;
        try {
            Assert.notNull(indexName, "索引名称不能为空");
            Assert.notNull(id, "索引文档ID不能为空");
            GetRequest request = new GetRequest(indexName, id);
            request.fetchSourceContext(new FetchSourceContext(false));
            request.storedFields("_none_");
            optFlag = restHighLevelClient.exists(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("根据ID判断索引文档是否存在异常。索引名称【{}】,索引文档ID【{}】", indexName, id, e);
        }
        return optFlag;
    }

    /**
     * 根据ID删除索引文档
     *
     * @param indexName 索引名称
     * @param id        文档id
     * @return
     * @throws IOException
     */
    public boolean deleteDocument(String indexName, String id) {
        boolean optFlag = Boolean.FALSE;
        try {
            Assert.notNull(indexName, "索引名称不能为空");
            Assert.notNull(id, "索引文档ID不能为空");
            GetRequest request = new GetRequest(indexName, id);
            request.fetchSourceContext(new FetchSourceContext(false));
            request.storedFields("_none_");
            optFlag = restHighLevelClient.exists(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("根据ID删除索引文档异常。索引名称【{}】,索引文档ID【{}】", indexName, id, e);
        }
        return optFlag;
    }

    /**
     * 根据ID修改索引文档
     *
     * @param indexName 索引名称
     * @param id        文档id
     * @param docMap    文档map
     * @return
     * @throws IOException
     */
    public boolean updateDocument(String indexName, String id, Map<String, Object> docMap) {
        boolean optFlag = Boolean.TRUE;
        try {
            Assert.notNull(indexName, "索引名称不能为空");
            Assert.notNull(id, "索引文档ID不能为空");
            Assert.notNull(docMap, "索引文档docMap不能为空");
            UpdateRequest request = new UpdateRequest(indexName, id).doc(docMap);
            restHighLevelClient.update(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("根据ID修改索引文档异常。索引名称【{}】,索引文档ID【{}】", indexName, id, e);
            optFlag = Boolean.FALSE;
        }
        return optFlag;
    }

    /**
     * 批量添加索引文档
     *
     * @param indexName 索引名称
     * @param docMaps   文档maps
     * @return
     * @throws IOException
     */
    public boolean bulkAddDocument(String indexName, List<Map<String, Object>> docMaps) {
        boolean optFlag = Boolean.TRUE;
        try {
            Assert.notNull(indexName, "索引名称不能为空");
            Assert.notNull(docMaps, "索引文档docMaps不能为空");

            BulkRequest bulkRequest = new BulkRequest();
            for (int i = 0; i < docMaps.size(); i++) {
                Map<String, Object> docMap = docMaps.get(i);
                bulkRequest.add(new IndexRequest(indexName).id(docMap.get("id") + "").source(docMaps).opType(DocWriteRequest.OpType.CREATE));
            }
            restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("批量添加索引文档异常。索引名称【{}】", indexName, e);
            optFlag = Boolean.FALSE;
        }
        return optFlag;
    }

    /**
     * 批量修改索引文档
     *
     * @param indexName 索引名称
     * @param docMaps   文档maps
     * @return
     * @throws IOException
     */
    public boolean bulkUpdateDocument(String indexName, List<Map<String, Object>> docMaps) {
        boolean optFlag = Boolean.TRUE;
        try {
            Assert.notNull(indexName, "索引名称不能为空");
            Assert.notNull(docMaps, "索引文档docMaps不能为空");

            BulkRequest bulkRequest = new BulkRequest();
            for (Map<String, Object> docMap : docMaps) {
                bulkRequest.add(new UpdateRequest(indexName, docMap.get("id") + "").doc(docMap));
            }
            restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("批量修改索引文档异常。索引名称【{}】", indexName, e);
            optFlag = Boolean.FALSE;
        }
        return optFlag;
    }

    /**
     * 批量修改索引文档
     *
     * @param indexName 索引名称
     * @param docMaps   文档maps
     * @return
     * @throws IOException
     */
    public boolean bulkDeleteDocument(String indexName, List<Map<String, Object>> docMaps) {
        boolean optFlag = Boolean.TRUE;
        try {
            Assert.notNull(indexName, "索引名称不能为空");
            Assert.notNull(docMaps, "索引文档docMaps不能为空");

            BulkRequest bulkRequest = new BulkRequest();
            for (Map<String, Object> docMap : docMaps) {
                bulkRequest.add(new DeleteRequest(indexName, docMap.get("id") + ""));
            }
            restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        } catch (Exception e) {
            log.error("批量修改索引文档异常。索引名称【{}】", indexName, e);
            optFlag = Boolean.FALSE;
        }
        return optFlag;
    }

    /**
     * 精准匹配--检索
     *
     * @param indexName 索引名称
     * @param fieldName 查询目标字段
     * @param terms     匹配条件
     * @return SQL翻译 select * from xxx_table where fieldName in(term1,term2)
     * GET /_search
     * {
     * "query": {
     * "terms" : { "user" : ["kimchy", "elasticsearch"]}
     * }
     * }
     * @throws IOException
     */
    public QueryResult searchForTermsQuery(String indexName, String fieldName, Object... terms) {
        QueryResult qr = new QueryResult(Boolean.TRUE);
        try {
            Assert.notNull(indexName, "索引名称indexName不能为空");
            Assert.notNull(fieldName, "查询目标属性fieldName不能为空");
            Assert.notNull(terms, "精准查询条件terms不能为空");
            SearchRequest searchRequest = new SearchRequest();
            // 设置要查询的索引名称
            searchRequest.indices(indexName);

            // 构造查询器
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.termsQuery(fieldName, terms));
            // 执行查询
            excuteQuery(searchRequest, sourceBuilder, qr);
        } catch (Exception e) {
            log.error("精准匹配检索异常。索引名称【{}】, terms查询字段【{}】,查询条件【{}】", indexName, fieldName, JSONObject.toJSONString(terms), e);
        }
        return qr;
    }

    /**
     * 精准匹配--分页检索
     *
     * @param indexName 索引名称
     * @param fieldName 查询目标字段
     * @param terms     匹配条件：匹配条件
     * @return SQL翻译 select * from xxx_table where fieldName in(term1,term2)
     * GET /_search
     * {
     * "query": {
     * "terms" : { "user" : ["kimchy", "elasticsearch"]}
     * }
     * }
     * @throws IOException
     */
    public QueryResult searchForTermsQuery(String indexName, String fieldName, int offset, int pageSize, Object... terms) {
        QueryResult qr = new QueryResult(Boolean.TRUE);
        try {
            Assert.notNull(indexName, "索引名称indexName不能为空");
            Assert.notNull(fieldName, "查询目标属性fieldName不能为空");
            Assert.notNull(terms, "精准查询条件terms不能为空");
            SearchRequest searchRequest = new SearchRequest();
            // 设置要查询的索引名称
            searchRequest.indices(indexName);

            // 构造查询器
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.termsQuery(fieldName, terms));
            // 设置分页
            sourceBuilder.from(offset).size(pageSize);
            // 执行查询
            excuteQuery(searchRequest, sourceBuilder, qr);
        } catch (Exception e) {
            log.error("精准匹配分页检索异常。索引名称【{}】, terms查询字段【{}】,查询条件【{}】", indexName, fieldName, JSONObject.toJSONString(terms), e);
        }
        return qr;
    }

    /**
     * 范围匹配--检索
     *
     * @param indexName 索引名称
     * @param fieldName 查询目标字段
     * @param terms     匹配条件：支持 gte(>=)、gt(>)、lte(<=)、lt(<)
     * @param format    日期转换格式
     * @return SQL翻译 select * from xxx_table where age >=18 and age<20
     * GET _search
     * {
     * "query": {
     * "range" : {
     * "age" : {
     * "gte" : 10,
     * "lte" : 20,
     * "boost" : 2.0
     * }
     * }
     * }
     * }
     * @throws IOException
     */
    public QueryResult searchForRangeQuery(String indexName, String fieldName, Map<String, Object> terms, String format) {
        QueryResult qr = new QueryResult(Boolean.TRUE);
        try {
            Assert.notNull(indexName, "索引名称indexName不能为空");
            Assert.notNull(fieldName, "查询目标属性fieldName不能为空");
            Assert.notNull(terms, "范围查询条件terms不能为空");
            SearchRequest searchRequest = new SearchRequest();
            // 设置要查询的索引名称
            searchRequest.indices(indexName);

            // 构造查询器
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            // 设置range查询条件
            RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(fieldName);
            for (String key : terms.keySet()) {
                switch (key) {
                    case "gte":
                        rangeQueryBuilder.gte(terms.get(key));
                        continue;
                    case "gt":
                        rangeQueryBuilder.gt(terms.get(key));
                        continue;
                    case "lte":
                        rangeQueryBuilder.lte(terms.get(key));
                        continue;
                    case "lt":
                        rangeQueryBuilder.lt(terms.get(key));
                        continue;
                    default:
                        break;
                }
            }
            // 设置转换规则 一般针对时间属性 dd/MM/yyyy||yyyy
            if (StringUtils.isNotEmpty(format)) {
                rangeQueryBuilder.format(format);
            }
            sourceBuilder.query(rangeQueryBuilder);
            // 执行查询
            excuteQuery(searchRequest, sourceBuilder, qr);
        } catch (Exception e) {
            log.error("单属性范围匹配检索异常。索引名称【{}】, range查询字段【{}】,查询条件【{}】", indexName, fieldName, JSONObject.toJSONString(terms), e);
        }
        return qr;
    }

    /**
     * 范围匹配-分页检索
     *
     * @param indexName 索引名称
     * @param fieldName 查询目标字段
     * @param offset    页码
     * @param pageSize  页大小
     * @param terms     匹配条件：支持 gte(>=)、gt(>)、lte(<=)、lt(<)
     * @param format    日期转换格式
     * @return SQL翻译 select * from xxx_table where age >=18 and age<20
     * GET _search
     * {
     * "query": {
     * "range" : {
     * "age" : {
     * "gte" : 10,
     * "lte" : 20,
     * "boost" : 2.0
     * }
     * }
     * }
     * }
     * @throws IOException
     */
    public QueryResult searchForRangeQuery(String indexName, String fieldName, int offset, int pageSize, Map<String, Object> terms, String format) {
        QueryResult qr = new QueryResult(Boolean.TRUE);
        try {
            Assert.notNull(indexName, "索引名称indexName不能为空");
            Assert.notNull(fieldName, "查询目标属性fieldName不能为空");
            Assert.notNull(terms, "范围查询条件terms不能为空");
            SearchRequest searchRequest = new SearchRequest();
            // 设置要查询的索引名称
            searchRequest.indices(indexName);

            // 构造查询器
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            // 设置range查询条件
            RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(fieldName);
            for (String key : terms.keySet()) {
                switch (key) {
                    case "gte":
                        rangeQueryBuilder.gte(terms.get(key));
                        continue;
                    case "gt":
                        rangeQueryBuilder.gt(terms.get(key));
                        continue;
                    case "lte":
                        rangeQueryBuilder.lte(terms.get(key));
                        continue;
                    case "lt":
                        rangeQueryBuilder.lt(terms.get(key));
                        continue;
                    default:
                        break;
                }
            }
            // 设置转换规则 一般针对时间属性 dd/MM/yyyy||yyyy
            if (StringUtils.isNotEmpty(format)) {
                rangeQueryBuilder.format(format);
            }
            sourceBuilder.query(rangeQueryBuilder);

            // 设置分页
            sourceBuilder.from(offset).size(pageSize);
            // 执行查询
            excuteQuery(searchRequest, sourceBuilder, qr);
        } catch (Exception e) {
            log.error("单属性范围匹配分页检索异常。索引名称【{}】, range查询字段【{}】,查询条件【{}】", indexName, fieldName, JSONObject.toJSONString(terms), e);
        }
        return qr;
    }

    /**
     * 前缀模糊匹配--检索
     *
     * @param indexName 索引名称
     * @param fieldName 查询目标字段
     * @param term      匹配条件
     * @return SQL翻译 select * from xxx_table where fieldName like 'prefix%'
     * GET /_search
     * { "query": {
     * "prefix" : { "user" : "ki" }
     * }
     * }
     * @throws IOException
     */
    public QueryResult searchForPrefixQuery(String indexName, String fieldName, String term) {
        QueryResult qr = new QueryResult(Boolean.TRUE);
        try {
            Assert.notNull(indexName, "索引名称indexName不能为空");
            Assert.notNull(fieldName, "查询目标属性fieldName不能为空");
            Assert.notNull(term, "前缀模糊检索条件term不能为空");
            SearchRequest searchRequest = new SearchRequest();
            // 设置要查询的索引名称
            searchRequest.indices(indexName);

            // 构造查询器
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.prefixQuery(fieldName, term));
            // 执行查询
            excuteQuery(searchRequest, sourceBuilder, qr);
        } catch (Exception e) {
            log.error(" 前缀模糊匹配检索异常。索引名称【{}】, terms查询字段【{}】,查询条件【{}】", indexName, fieldName, term, e);
        }
        return qr;
    }

    /**
     * 前缀模糊匹配--分页检索
     *
     * @param indexName 索引名称
     * @param fieldName 查询目标字段
     * @param offset    页码
     * @param pageSize  页大小
     * @param term      匹配条件：支持 gte(>=)、gt(>)、lte(<=)、lt(<)
     * @return SQL翻译 select * from xxx_table where fieldName like 'prefix%'
     * GET /_search
     * { "query": {
     * "prefix" : { "user" : "ki" }
     * }
     * }
     * @throws IOException
     */
    public QueryResult searchForPrefixQuery(String indexName, String fieldName, int offset, int pageSize, String term) {
        QueryResult qr = new QueryResult(Boolean.TRUE);
        try {
            Assert.notNull(indexName, "索引名称indexName不能为空");
            Assert.notNull(fieldName, "查询目标属性fieldName不能为空");
            Assert.notNull(term, "前缀模糊检索条件term不能为空");
            SearchRequest searchRequest = new SearchRequest();
            // 设置要查询的索引名称
            searchRequest.indices(indexName);

            // 构造查询器
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.prefixQuery(fieldName, term));
            //设置分页
            sourceBuilder.from(offset).size(pageSize);
            // 执行查询
            excuteQuery(searchRequest, sourceBuilder, qr);
        } catch (Exception e) {
            log.error(" 前缀模糊匹配分页检索异常。索引名称【{}】, terms查询字段【{}】,查询条件【{}】", indexName, fieldName, term, e);
        }
        return qr;
    }

    /**
     * 通配符模糊匹配--检索
     *
     * @param indexName 索引名称
     * @param fieldName 查询目标字段
     * @param term      匹配条件
     * @return SQL翻译 select * from xxx_table where fieldName like 'xxx%xxx'
     * GET /_search
     * {
     * "query": {
     * "wildcard" : { "user" : "ki*y" }
     * }
     * }
     * @throws IOException
     */
    public QueryResult searchForWildcardQuery(String indexName, String fieldName, String term) {
        QueryResult qr = new QueryResult(Boolean.TRUE);
        try {
            Assert.notNull(indexName, "索引名称indexName不能为空");
            Assert.notNull(fieldName, "查询目标属性fieldName不能为空");
            Assert.notNull(term, "通配符模糊检索条件term不能为空");
            SearchRequest searchRequest = new SearchRequest();
            // 设置要查询的索引名称
            searchRequest.indices(indexName);

            // 构造查询器
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.wildcardQuery(fieldName, term));
            // 执行查询
            excuteQuery(searchRequest, sourceBuilder, qr);
        } catch (Exception e) {
            log.error(" 通配符模糊匹配检索异常。索引名称【{}】, term查询字段【{}】,查询条件【{}】", indexName, fieldName, term, e);
        }
        return qr;
    }

    /**
     * 通配符模糊匹配--分页检索
     *
     * @param indexName 索引名称
     * @param fieldName 查询目标字段
     * @param offset    页码
     * @param pageSize  页大小
     * @param term      匹配条件
     * @return SQL翻译 select * from xxx_table where fieldName like 'xxx%xxx'
     * GET /_search
     * {
     * "query": {
     * "wildcard" : { "user" : "ki*y" }
     * }
     * }
     * @throws IOException
     */
    public QueryResult searchForWildcardQuery(String indexName, String fieldName, int offset, int pageSize, String term) {
        QueryResult qr = new QueryResult(Boolean.TRUE);
        try {
            Assert.notNull(indexName, "索引名称indexName不能为空");
            Assert.notNull(fieldName, "查询目标属性fieldName不能为空");
            Assert.notNull(term, "通配符模糊检索条件term不能为空");
            SearchRequest searchRequest = new SearchRequest();
            // 设置要查询的索引名称
            searchRequest.indices(indexName);

            // 构造查询器
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.wildcardQuery(fieldName, term));
            // 设置分页
            sourceBuilder.from(offset).size(pageSize);
            // 执行查询
            excuteQuery(searchRequest, sourceBuilder, qr);
        } catch (Exception e) {
            log.error(" 通配符模糊匹配检索异常。索引名称【{}】, term查询字段【{}】,查询条件【{}】", indexName, fieldName, term, e);
        }
        return qr;
    }

    /**
     * 模糊匹配--检索
     *
     * @param indexName 索引名称
     * @param fieldName 查询目标字段
     * @param term      匹配条件
     * @return SQL翻译 select * from xxx_table where fieldName like '%term%'
     * GET /_search
     * {
     * "query": {
     * "fuzzy" : { "user" : "ki" }
     * }
     * }
     * @throws IOException
     */
    public QueryResult searchForFuzzyQuery(String indexName, String fieldName, Object term) {
        QueryResult qr = new QueryResult(Boolean.TRUE);
        try {
            Assert.notNull(indexName, "索引名称indexName不能为空");
            Assert.notNull(fieldName, "查询目标属性fieldName不能为空");
            Assert.notNull(term, "模糊检索条件term不能为空");
            SearchRequest searchRequest = new SearchRequest();
            // 设置要查询的索引名称
            searchRequest.indices(indexName);

            // 构造查询器
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.fuzzyQuery(fieldName, term));
            // 执行查询
            excuteQuery(searchRequest, sourceBuilder, qr);
        } catch (Exception e) {
            log.error(" 模糊匹配检索异常。索引名称【{}】, term查询字段【{}】,查询条件【{}】", indexName, fieldName, term, e);
        }
        return qr;
    }

    /**
     * 模糊匹配--分页检索
     *
     * @param indexName 索引名称
     * @param fieldName 查询目标字段
     * @param term      匹配条件
     * @return SQL翻译 select * from xxx_table where fieldName like '%term%'
     * GET /_search
     * {
     * "query": {
     * "fuzzy" : { "user" : "ki" }
     * }
     * }
     * @throws IOException
     */
    public QueryResult searchForFuzzyQuery(String indexName, String fieldName, int offset, int pageSize, Object term) {
        QueryResult qr = new QueryResult(Boolean.TRUE);
        try {
            Assert.notNull(indexName, "索引名称indexName不能为空");
            Assert.notNull(fieldName, "查询目标属性fieldName不能为空");
            Assert.notNull(term, "模糊检索条件term不能为空");
            SearchRequest searchRequest = new SearchRequest();
            // 设置要查询的索引名称
            searchRequest.indices(indexName);

            // 构造查询器
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.fuzzyQuery(fieldName, term));
            // 设置分页
            sourceBuilder.from(offset).size(pageSize);
            // 执行查询
            excuteQuery(searchRequest, sourceBuilder, qr);
        } catch (Exception e) {
            log.error("模糊匹配检索异常。索引名称【{}】, term查询字段【{}】,查询条件【{}】", indexName, fieldName, term, e);
        }
        return qr;
    }

    /**
     * 根据ids匹配--检索
     *
     * @param indexName 索引名称
     * @param fieldName 查询目标字段
     * @param terms     匹配条件
     * @return SQL翻译 select * from xxx_table where id in(id1,id2...)
     * GET /_search
     * {
     * "query": {
     * "ids" : {
     * "values" : ["1", "2", "3"]
     * }
     * }
     * }
     * @throws IOException
     */
    public QueryResult searchForIdsQuery(String indexName, String fieldName, String... terms) {
        QueryResult qr = new QueryResult(Boolean.TRUE);
        try {
            Assert.notNull(indexName, "索引名称indexName不能为空");
            Assert.notNull(fieldName, "查询目标属性fieldName不能为空");
            Assert.notNull(terms, "ids检索条件term不能为空");
            SearchRequest searchRequest = new SearchRequest();
            // 设置要查询的索引名称
            searchRequest.indices(indexName);

            // 构造查询器
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.idsQuery(terms));
            // 执行查询
            excuteQuery(searchRequest, sourceBuilder, qr);
        } catch (Exception e) {
            log.error("根据ids匹配检索异常。索引名称【{}】, term查询字段【{}】,查询条件【{}】", indexName, fieldName, JSONObject.toJSONString(terms), e);
        }
        return qr;
    }

    /**
     * 根据ids匹配--分页检索
     *
     * @param indexName 索引名称
     * @param fieldName 查询目标字段
     * @param offset    页码
     * @param pageSize  页大小
     * @param terms     匹配条件
     * @return SQL翻译 select * from xxx_table where id in(id1,id2...)
     * GET /_search
     * {
     * "query": {
     * "ids" : {
     * "values" : ["1", "2", "3"]
     * }
     * }
     * }
     * @throws IOException
     */
    public QueryResult searchForIdsQuery(String indexName, String fieldName, int offset, int pageSize, String... terms) {
        QueryResult qr = new QueryResult(Boolean.TRUE);
        try {
            Assert.notNull(indexName, "索引名称indexName不能为空");
            Assert.notNull(fieldName, "查询目标属性fieldName不能为空");
            Assert.notNull(terms, "ids检索条件term不能为空");
            SearchRequest searchRequest = new SearchRequest();
            // 设置要查询的索引名称
            searchRequest.indices(indexName);

            // 构造查询器
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.idsQuery(terms));
            // 设置分页
            sourceBuilder.from(offset).size(pageSize);
            // 执行查询
            excuteQuery(searchRequest, sourceBuilder, qr);
        } catch (Exception e) {
            log.error("根据ids匹配分页检索异常。索引名称【{}】, term查询字段【{}】,查询条件【{}】", indexName, fieldName, JSONObject.toJSONString(terms), e);
        }
        return qr;
    }

    /**######################################## QUERY DSL【复合查询】  #################################################
     * https://www.elastic.co/guide/en/elasticsearch/reference/7.0/query-dsl-match-all-query.html
     *
     */

    /**
     * 复合匹配--检索
     *
     * @param indexName 索引名称
     * @param terms     匹配条件：支持 gte(>=)、gt(>)、lte(<=)、lt(<)
     *                  must 必须出现在匹配的文档中，并有助于得分
     *                  filter 必须出现在匹配的文档中。但是不同于 must查询的分数将被忽略。过滤器子句在过滤器上下文中执行，这意味着忽略评分并考虑使用子句进行高速缓存
     *                  should 应出现在匹配的文档中
     *                  must_not不得出现在匹配的文档中
     * @return SQL翻译 select * from xxx_table where age >=18 and age<20 and name not in(name1,name2)
     * POST _search
     * {
     * "query": {
     * "bool" : {
     * "must" : {
     * "term" : { "user" : "kimchy" }
     * },
     * "filter": {
     * "term" : { "tag" : "tech" }
     * },
     * "must_not" : {
     * "range" : {
     * "age" : { "gte" : 10, "lte" : 20 }
     * }
     * },
     * "should" : [
     * { "term" : { "tag" : "wow" } },
     * { "term" : { "tag" : "elasticsearch" } }
     * ],
     * "minimum_should_match" : 1,
     * "boost" : 1.0
     * }
     * }
     * }
     * @throws IOException
     */
    public QueryResult searchForBoolQuery(String indexName, Map<String, List<QueryBuilder>> terms) {
        QueryResult qr = new QueryResult(Boolean.TRUE);
        try {
            Assert.notNull(indexName, "索引名称indexName不能为空");
            Assert.notNull(terms, "范围查询条件terms不能为空");
            SearchRequest searchRequest = new SearchRequest();
            // 设置要查询的索引名称
            searchRequest.indices(indexName);

            // 构造查询器
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            // 设置复合查询
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            for (String key : terms.keySet()) {
                switch (key) {
                    case "must":
                        List<QueryBuilder> mustQbs = terms.get(key);
                        for (QueryBuilder qb : mustQbs) {
                            boolQueryBuilder.must(qb);
                        }
                        continue;
                    case "filter":
                        List<QueryBuilder> filterQbs = terms.get(key);
                        for (QueryBuilder qb : filterQbs) {
                            boolQueryBuilder.filter(qb);
                        }
                        continue;
                    case "mustNot":
                        List<QueryBuilder> mustNotQbs = terms.get(key);
                        for (QueryBuilder qb : mustNotQbs) {
                            boolQueryBuilder.mustNot(qb);
                        }
                        continue;
                    case "should":
                        List<QueryBuilder> shouldQbs = terms.get(key);
                        for (QueryBuilder qb : shouldQbs) {
                            boolQueryBuilder.should(qb);
                        }
                        continue;
                    default:
                        break;
                }
            }
            sourceBuilder.query(boolQueryBuilder);

            // 执行查询
            excuteQuery(searchRequest, sourceBuilder, qr);
        } catch (Exception e) {
            log.error("复合匹配检索异常。索引名称【{}】", indexName, e);
        }
        return qr;
    }

    /**
     * 复合匹配--分页检索
     *
     * @param indexName 索引名称
     * @param offset    页码
     * @param pageSize  页大小
     * @param terms     匹配条件：支持 gte(>=)、gt(>)、lte(<=)、lt(<)
     *                  must 必须出现在匹配的文档中，并有助于得分
     *                  filter 必须出现在匹配的文档中。但是不同于 must查询的分数将被忽略。过滤器子句在过滤器上下文中执行，这意味着忽略评分并考虑使用子句进行高速缓存
     *                  should 应出现在匹配的文档中
     *                  must_not不得出现在匹配的文档中
     * @return SQL翻译 select * from xxx_table where age >=18 and age<20 and name not in(name1,name2)
     * POST _search
     * {
     * "query": {
     * "bool" : {
     * "must" : {
     * "term" : { "user" : "kimchy" }
     * },
     * "filter": {
     * "term" : { "tag" : "tech" }
     * },
     * "must_not" : {
     * "range" : {
     * "age" : { "gte" : 10, "lte" : 20 }
     * }
     * },
     * "should" : [
     * { "term" : { "tag" : "wow" } },
     * { "term" : { "tag" : "elasticsearch" } }
     * ],
     * "minimum_should_match" : 1,
     * "boost" : 1.0
     * }
     * }
     * }
     * @throws IOException
     */
    public QueryResult searchForBoolQuery(String indexName, int offset, int pageSize, Map<String, List<QueryBuilder>> terms) {
        QueryResult qr = new QueryResult(Boolean.TRUE);
        try {
            Assert.notNull(indexName, "索引名称indexName不能为空");
            Assert.notNull(terms, "范围查询条件terms不能为空");
            SearchRequest searchRequest = new SearchRequest();
            // 设置要查询的索引名称
            searchRequest.indices(indexName);

            // 构造查询器
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            // 设置复合查询
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            for (String key : terms.keySet()) {
                switch (key) {
                    case "must":
                        List<QueryBuilder> mustQbs = terms.get(key);
                        for (QueryBuilder qb : mustQbs) {
                            boolQueryBuilder.must(qb);
                        }
                        continue;
                    case "filter":
                        List<QueryBuilder> filterQbs = terms.get(key);
                        for (QueryBuilder qb : filterQbs) {
                            boolQueryBuilder.filter(qb);
                        }
                        continue;
                    case "mustNot":
                        List<QueryBuilder> mustNotQbs = terms.get(key);
                        for (QueryBuilder qb : mustNotQbs) {
                            boolQueryBuilder.mustNot(qb);
                        }
                        continue;
                    case "should":
                        List<QueryBuilder> shouldQbs = terms.get(key);
                        for (QueryBuilder qb : shouldQbs) {
                            boolQueryBuilder.should(qb);
                        }
                        continue;
                    default:
                        break;
                }
            }
            sourceBuilder.query(boolQueryBuilder);
            // 设置分页
            sourceBuilder.from(offset).size(pageSize);

            // 执行查询
            excuteQuery(searchRequest, sourceBuilder, qr);

        } catch (Exception e) {
            log.error("复合匹配分页检索异常。索引名称【{}】", indexName, e);
        }
        return qr;
    }

    /**
     * 复合匹配--高亮检索
     *
     * @param indexName 索引名称
     * @param terms     匹配条件：支持 gte(>=)、gt(>)、lte(<=)、lt(<)
     *                  must 必须出现在匹配的文档中，并有助于得分
     *                  filter 必须出现在匹配的文档中。但是不同于 must查询的分数将被忽略。过滤器子句在过滤器上下文中执行，这意味着忽略评分并考虑使用子句进行高速缓存
     *                  should 应出现在匹配的文档中
     *                  must_not不得出现在匹配的文档中
     * @return SQL翻译 select * from xxx_table where age >=18 and age<20 and name not in(name1,name2)
     * POST _search
     * {
     * "query": {
     * "bool" : {
     * "must" : {
     * "term" : { "user" : "kimchy" }
     * },
     * "filter": {
     * "term" : { "tag" : "tech" }
     * },
     * "must_not" : {
     * "range" : {
     * "age" : { "gte" : 10, "lte" : 20 }
     * }
     * },
     * "should" : [
     * { "term" : { "tag" : "wow" } },
     * { "term" : { "tag" : "elasticsearch" } }
     * ],
     * "minimum_should_match" : 1,
     * "boost" : 1.0
     * }
     * },
     * "highlight": {
     * "pre_tags": "<em>",
     * "post_tags": "</em>",
     * "fields": {
     * "fullText":{
     * "type": "unified",
     * "fragment_size" : 300,
     * "number_of_fragments" : 0,
     * "no_match_size": 2
     * }
     * }
     * }
     * }
     * @throws IOException
     */
    public QueryResult searchForHighlightBoolQuery(String indexName, Map<String, List<QueryBuilder>> terms) {
        QueryResult qr = new QueryResult(Boolean.TRUE);
        try {
            Assert.notNull(indexName, "索引名称indexName不能为空");
            Assert.notNull(terms, "范围查询条件terms不能为空");
            SearchRequest searchRequest = new SearchRequest();
            // 设置要查询的索引名称
            searchRequest.indices(indexName);

            // 构造查询器
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            // 设置复合查询
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            for (String key : terms.keySet()) {
                switch (key) {
                    case "must":
                        List<QueryBuilder> mustQbs = terms.get(key);
                        for (QueryBuilder qb : mustQbs) {
                            boolQueryBuilder.must(qb);
                        }
                        continue;
                    case "filter":
                        List<QueryBuilder> filterQbs = terms.get(key);
                        for (QueryBuilder qb : filterQbs) {
                            boolQueryBuilder.filter(qb);
                        }
                        continue;
                    case "mustNot":
                        List<QueryBuilder> mustNotQbs = terms.get(key);
                        for (QueryBuilder qb : mustNotQbs) {
                            boolQueryBuilder.mustNot(qb);
                        }
                        continue;
                    case "should":
                        List<QueryBuilder> shouldQbs = terms.get(key);
                        for (QueryBuilder qb : shouldQbs) {
                            boolQueryBuilder.should(qb);
                        }
                        continue;
                    default:
                        break;
                }
            }
            sourceBuilder.query(boolQueryBuilder);

            // 高亮构造器
            HighlightBuilder highlightBuilder = new HighlightBuilder();

            // 全局设置
            highlightBuilder.numOfFragments(1);
            highlightBuilder.fragmentSize(900);

            // 自定义高亮标签
            highlightBuilder.preTags("<em class='searcHighlight'>");
            highlightBuilder.postTags("</em>");

            HighlightBuilder.Field highlightFullText = new HighlightBuilder.Field("fullText");
            // 设置显示器。支持unified、plain、fvh。默认unified
            highlightFullText.highlighterType("fvh");
            // 设置片段长度,默认100
            highlightFullText.fragmentOffset(300);
            // 设置返回的片段数, 默认5
            highlightFullText.numOfFragments(10);
            highlightBuilder.field(highlightFullText);

            // 添加更多高亮字段
            //HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("ip_addr");
            //highlightBuilder.field(highlightUser);

            // 设置高亮构造器
            sourceBuilder.highlighter(highlightBuilder);

            // 执行查询
            excuteQuery(searchRequest, sourceBuilder, qr);

        } catch (Exception e) {
            log.error("复合匹配高亮检索异常。索引名称【{}】", indexName, e);
        }
        return qr;
    }

    /**
     * 复合匹配--高亮分页检索
     *
     * @param indexName 索引名称
     * @param offset    页码
     * @param pageSize  页大小
     * @param terms     匹配条件：支持 gte(>=)、gt(>)、lte(<=)、lt(<)
     *                  must 必须出现在匹配的文档中，并有助于得分
     *                  filter 必须出现在匹配的文档中。但是不同于 must查询的分数将被忽略。过滤器子句在过滤器上下文中执行，这意味着忽略评分并考虑使用子句进行高速缓存
     *                  should 应出现在匹配的文档中
     *                  must_not不得出现在匹配的文档中
     * @return SQL翻译 select * from xxx_table where age >=18 and age<20 and name not in(name1,name2)
     * POST _search
     * {
     * "query": {
     * "bool" : {
     * "must" : {
     * "term" : { "user" : "kimchy" }
     * },
     * "filter": {
     * "term" : { "tag" : "tech" }
     * },
     * "must_not" : {
     * "range" : {
     * "age" : { "gte" : 10, "lte" : 20 }
     * }
     * },
     * "should" : [
     * { "term" : { "tag" : "wow" } },
     * { "term" : { "tag" : "elasticsearch" } }
     * ],
     * "minimum_should_match" : 1,
     * "boost" : 1.0
     * }
     * },
     * "highlight": {
     * "pre_tags": "<em>",
     * "post_tags": "</em>",
     * "fields": {
     * "fullText":{
     * "type": "unified",
     * "fragment_size" : 300,
     * "number_of_fragments" : 0,
     * "no_match_size": 2
     * }
     * }
     * }
     * }
     * @throws IOException
     */
    public QueryResult searchForHighlightBoolQuery(String indexName, int offset, int pageSize, Map<String, List<QueryBuilder>> terms) {
        QueryResult qr = new QueryResult(Boolean.TRUE);
        try {
            Assert.notNull(indexName, "索引名称indexName不能为空");
            Assert.notNull(terms, "范围查询条件terms不能为空");
            SearchRequest searchRequest = new SearchRequest();
            // 设置要查询的索引名称
            searchRequest.indices(indexName);

            // 构造查询器
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            // 设置复合查询
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            for (String key : terms.keySet()) {
                switch (key) {
                    case "must":
                        List<QueryBuilder> mustQbs = terms.get(key);
                        for (QueryBuilder qb : mustQbs) {
                            boolQueryBuilder.must(qb);
                        }
                        continue;
                    case "filter":
                        List<QueryBuilder> filterQbs = terms.get(key);
                        for (QueryBuilder qb : filterQbs) {
                            boolQueryBuilder.filter(qb);
                        }
                        continue;
                    case "mustNot":
                        List<QueryBuilder> mustNotQbs = terms.get(key);
                        for (QueryBuilder qb : mustNotQbs) {
                            boolQueryBuilder.mustNot(qb);
                        }
                        continue;
                    case "should":
                        List<QueryBuilder> shouldQbs = terms.get(key);
                        for (QueryBuilder qb : shouldQbs) {
                            boolQueryBuilder.should(qb);
                        }
                        continue;
                    default:
                        break;
                }
            }
            sourceBuilder.query(boolQueryBuilder);

            // 高亮构造器
            HighlightBuilder highlightBuilder = new HighlightBuilder();

            // 全局设置
            highlightBuilder.numOfFragments(5);
            highlightBuilder.fragmentSize(100);

            // 自定义高亮标签
            highlightBuilder.preTags("<em class='searcHighlight'>");
            highlightBuilder.postTags("</em>");

            HighlightBuilder.Field highlightFullText = new HighlightBuilder.Field("fullText");
            // 设置显示器。支持unified、plain、fvh。默认unified
            highlightFullText.highlighterType("fvh");
            // 设置片段长度,默认100
            highlightFullText.fragmentOffset(100);
            // 设置返回的片段数, 默认5
            highlightFullText.numOfFragments(5);
            highlightBuilder.field(highlightFullText);

            // 添加更多高亮字段
            //HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("ip_addr");
            //highlightBuilder.field(highlightUser);

            // 设置高亮构造器
            sourceBuilder.highlighter(highlightBuilder);

            // 设置分页
            sourceBuilder.from(offset).size(pageSize);

            // 执行查询
            excuteQuery(searchRequest, sourceBuilder, qr);

        } catch (Exception e) {
            log.error("复合匹配高亮分页检索异常。索引名称【{}】", indexName, e);
        }
        return qr;
    }

    /**
     * 执行查询并设置相关参数
     *
     * @throws IOException
     */
    public void excuteQuery(SearchRequest searchRequest, SearchSourceBuilder sourceBuilder, QueryResult qr) throws IOException {
        // 设置超时
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        // 按查询评分降序 排序支持四种：Field-, Score-, GeoDistance-, ScriptSortBuilder
        sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));

        // 设置查询器
        searchRequest.source(sourceBuilder);

        // 执行查询
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        // 封装查询结果
        doQueryResult(searchResponse, qr);
    }

    /**
     * 查询结果封装
     */
    public void doQueryResult(SearchResponse searchResponse, QueryResult qr) {
        // 查询耗时
        TimeValue took = searchResponse.getTook();
        qr.setTook(took.getMillis());

        // 是否超时
        qr.setTimedout(searchResponse.isTimedOut());

        // 查询总数
        qr.setHitsTotal(searchResponse.getHits().getTotalHits().value);

        // 最高评分
        qr.setMaxScore(searchResponse.getHits().getMaxScore());

        // 分片信息
        Map<String, Integer> shardsInfo = new HashMap<String, Integer>();
        shardsInfo.put("total", searchResponse.getTotalShards());
        shardsInfo.put("successful", searchResponse.getSuccessfulShards());
        shardsInfo.put("skipped", searchResponse.getSkippedShards());
        shardsInfo.put("failed", searchResponse.getFailedShards());
        qr.setShardsInfo(shardsInfo);

        // 获取查询结果
        List<Map<String, Object>> hitsBody = new ArrayList<Map<String, Object>>();
        SearchHits termhts = searchResponse.getHits();
        for (SearchHit hit : termhts) {
            Map<String, Object> hitMap = hit.getSourceAsMap();

            // 高亮内容封装
            if (hitMap != null) {
                Map<String, HighlightField> highMap = hit.getHighlightFields();
                Map<String, String> highTextMap = new HashMap<String, String>();
                if (highMap != null) {
                    for (String highKey : highMap.keySet()) {
                        String fieldName = highMap.get(highKey).getName();
                        Text highText = (highMap.get(highKey).fragments())[0];
                        highTextMap.put(fieldName, highText.toString());
                    }

                    hitMap.put("highlight", highTextMap);
                }
            }
            hitsBody.add(hitMap);
        }
        qr.setHitsBody(hitsBody);
    }
}
