package com.hadoop.elasticsearch.response;

import java.util.List;
import java.util.Map;

/**
 * 查询结果封装类
 *
 * @author He.Yong
 * @since 2021-06-29 17:17:15
 */
public class QueryResult {

    /**
     * 查询是否成功true=成功，false=失败
     */
    private boolean succesful;
    /**
     * 查询耗时
     */
    private long took;
    /**
     * 是否超时
     */
    private boolean timedout;
    /**
     * 查询总数
     */
    private long hitsTotal;
    /**
     * 最高评分
     */
    private float maxScore;
    /**
     * 分片信息
     * total : 分片总数
     * successful : 成功查询的分片数
     * skipped" : 跳过查询的分片数
     * failed" : 失败查询的分片数
     */
    private Map<String, Integer> shardsInfo;
    /**
     * 查询结果
     */
    private List<Map<String, Object>> hitsBody;

    public QueryResult() {
    }

    public QueryResult(boolean succesful) {
        this.succesful = succesful;
    }

    public boolean getSuccesful() {
        return succesful;
    }

    public void setSuccesful(boolean succesful) {
        this.succesful = succesful;
    }

    public long getTook() {
        return took;
    }

    public void setTook(long took) {
        this.took = took;
    }

    public boolean isTimedout() {
        return timedout;
    }

    public void setTimedout(boolean timedout) {
        this.timedout = timedout;
    }

    public long getHitsTotal() {
        return hitsTotal;
    }

    public void setHitsTotal(long hitsTotal) {
        this.hitsTotal = hitsTotal;
    }

    public float getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(float maxScore) {
        this.maxScore = maxScore;
    }

    public Map<String, Integer> getShardsInfo() {
        return shardsInfo;
    }

    public void setShardsInfo(Map<String, Integer> shardsInfo) {
        this.shardsInfo = shardsInfo;
    }

    public List<Map<String, Object>> getHitsBody() {
        return hitsBody;
    }

    public void setHitsBody(List<Map<String, Object>> hitsBody) {
        this.hitsBody = hitsBody;
    }
}