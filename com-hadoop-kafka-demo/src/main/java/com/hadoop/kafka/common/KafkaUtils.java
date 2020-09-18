package com.hadoop.kafka.common;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * kafka工具类
 */
@Component
public class KafkaUtils {

    @Autowired
    private AdminClient adminClient;

    /**
     * 组装单个topic
     *
     * @param topic
     * @return
     */
    public boolean createSingleTopic(String topic, int numPartitions, short replicationFactor) {

        NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
        List<NewTopic> newTopics = new ArrayList<>(1);
        newTopics.add(newTopic);
        return createTopics(newTopics, null);
    }

    /**
     * 创建topic  通用方法
     *
     * @param newTopics
     * @param createTopicsOptions
     * @return
     */
    public boolean createTopics(List<NewTopic> newTopics, CreateTopicsOptions createTopicsOptions) {

        if (createTopicsOptions == null) {
            createTopicsOptions = new CreateTopicsOptions();
            createTopicsOptions.timeoutMs(1000);
        }

        CreateTopicsResult results = adminClient.createTopics(newTopics, createTopicsOptions);
        KafkaFuture<Void> kafkaFuture = results.all();
        return kafkaFuture.isDone();
    }

    /**
     * 删除topics
     *
     * @param name
     * @return
     */
    public boolean deleteTopic(String... name) {

        if (name == null || name.length == 0) {
            return true;
        }

        List<String> topics = Arrays.asList(name);
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topics);
        return deleteTopicsResult.all().isDone();
    }

    /**
     * 获取topic 详情
     *
     * @param topics
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public Map<String, TopicDescription> descriptTopic(String... topics) throws InterruptedException, ExecutionException {
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(topics));
        return describeTopicsResult.all().get();

    }

    /**
     * 查询主题名
     *
     * @return
     */
    public List<String> queryAllTopic() {

        ListTopicsResult listTopicsResult = adminClient.listTopics();
        KafkaFuture<Collection<TopicListing>> kafkaFuture = listTopicsResult.listings();

        Collection<TopicListing> collections;
        List<String> topics = null;
        try {
            collections = kafkaFuture.get();
            if (collections != null && collections.size() != 0) {
                topics = new ArrayList<>(collections.size());
                for (TopicListing topicListing : collections) {
                    topics.add(topicListing.name());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return topics;
    }
}