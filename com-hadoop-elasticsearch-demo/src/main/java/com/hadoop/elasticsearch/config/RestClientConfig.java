package com.hadoop.elasticsearch.config;

import com.hadoop.elasticsearch.factory.ElasticRestClientFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * ElasticSearch 配置
 *
 * @author He.Yong
 * @since 2021-06-29 17:15:19
 */
@Configuration
public class RestClientConfig {

    /**
     * elasticsearch 连接地址多个地址使用,分隔
     */
    @Value("${elasticsearch.address}")
    private String[] address;

    /**
     * schema
     */
    @Value("${elasticsearch.client.schema}")
    private String schema;

    /**
     * 连接目标url最大超时
     */
    @Value("${elasticsearch.client.connectTimeOut}")
    private Integer connectTimeOut;

    /**
     * 等待响应（读数据）最大超时
     */
    @Value("${elasticsearch.client.socketTimeOut}")
    private Integer socketTimeOut;

    /**
     * 从连接池中获取可用连接最大超时时间
     */
    @Value("${elasticsearch.client.connectionRequestTime}")
    private Integer connectionRequestTime;

    /**
     * 连接池中的最大连接数
     */
    @Value("${elasticsearch.client.maxConnectNum}")
    private Integer maxConnectNum;

    /**
     * 连接同一个route最大的并发数
     */
    @Value("${elasticsearch.client.maxConnectPerRoute}")
    private Integer maxConnectPerRoute;

    @Bean
    public HttpHost[] httpHost() {
        HttpHost[] httpHosts = new HttpHost[address.length];
        for (int i = 0; i < address.length; i++) {
            String[] ipAddr = address[i].split(":");
            httpHosts[i] = new HttpHost(ipAddr[0], Integer.parseInt(ipAddr[1]), schema);
        }
        return httpHosts;
    }

    @Bean(initMethod = "init", destroyMethod = "close")
    public ElasticRestClientFactory getFactory() {
        return ElasticRestClientFactory.
                build(httpHost(), connectTimeOut, socketTimeOut, connectionRequestTime, maxConnectNum, maxConnectPerRoute);
    }

    @Bean
    @Scope("singleton")
    public RestClient getRestClient() {
        return getFactory().getClient();
    }

    @Bean
    @Scope("singleton")
    public RestHighLevelClient getRestHighClient() {
        return getFactory().getRestHighClient();
    }
}
