package com.hadoop.elasticsearch.factory;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * ElasticRestClientFactory工厂类
 *
 * @author He.Yong
 * @since 2021-06-29 17:22:16
 */
public class ElasticRestClientFactory {

    private static final Logger log = LoggerFactory.getLogger(ElasticRestClientFactory.class);

    // 连接目标url最大超时
    public static int CONNECT_TIMEOUT_MILLIS = 3000;
    // 等待响应（读数据）最大超时
    public static int SOCKET_TIMEOUT_MILLIS = 6000;
    // 从连接池中获取可用连接最大超时时间
    public static int CONNECTION_REQUEST_TIMEOUT_MILLIS = 2000;

    // 连接池中的最大连接数
    public static int MAX_CONN_TOTAL = 15;
    // 连接同一个route最大的并发数
    public static int MAX_CONN_PER_ROUTE = 10;

    private static HttpHost[] HTTP_HOST;
    private RestClientBuilder builder;
    private RestClient restClient;
    private RestHighLevelClient restHighLevelClient;

    private static final ElasticRestClientFactory restClientFactory = new ElasticRestClientFactory();

    private ElasticRestClientFactory() {
    }

    public static ElasticRestClientFactory build(HttpHost[] httpHost, Integer maxConnectNum, Integer maxConnectPerRoute) {
        HTTP_HOST = httpHost;
        MAX_CONN_TOTAL = maxConnectNum;
        MAX_CONN_PER_ROUTE = maxConnectPerRoute;
        return restClientFactory;
    }

    public static ElasticRestClientFactory build(HttpHost[] httpHost, Integer connectTimeOut, Integer socketTimeOut,
                                                 Integer connectionRequestTime, Integer maxConnectNum, Integer maxConnectPerRoute) {
        HTTP_HOST = httpHost;
        CONNECT_TIMEOUT_MILLIS = connectTimeOut;
        SOCKET_TIMEOUT_MILLIS = socketTimeOut;
        CONNECTION_REQUEST_TIMEOUT_MILLIS = connectionRequestTime;
        MAX_CONN_TOTAL = maxConnectNum;
        MAX_CONN_PER_ROUTE = maxConnectPerRoute;
        return restClientFactory;
    }


    public void init() {
        builder = RestClient.builder(HTTP_HOST);
        setConnectTimeOutConfig();
        setMutiConnectConfig();
        restClient = builder.build();
        restHighLevelClient = new RestHighLevelClient(builder);
        log.info("-----Elasticsearch highLevelRestClient 客户端初始化成功!-----");
    }

    // 配置连接延时时间
    public void setConnectTimeOutConfig() {
        builder.setRequestConfigCallback(requestConfigBuilder -> {
            requestConfigBuilder.setConnectTimeout(CONNECT_TIMEOUT_MILLIS);
            requestConfigBuilder.setSocketTimeout(SOCKET_TIMEOUT_MILLIS);
            requestConfigBuilder.setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT_MILLIS);
            return requestConfigBuilder;
        });
    }

    // 使用异步httpclient时设置并发连接数
    public void setMutiConnectConfig() {
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setMaxConnTotal(MAX_CONN_TOTAL);
            httpClientBuilder.setMaxConnPerRoute(MAX_CONN_PER_ROUTE);
            return httpClientBuilder;
        });
    }

    public RestClient getClient() {
        return restClient;
    }

    public RestHighLevelClient getRestHighClient() {
        return restHighLevelClient;
    }

    public void close() {
        if (restClient != null) {
            try {
                restClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        log.info("----Elasticsearch highLevelRestClient 已经关闭----");
    }
}