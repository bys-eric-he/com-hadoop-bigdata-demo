package com.hadoop.hive.service.dwd;

import com.hadoop.hive.service.ETLService;

/**
 * DWD层 明细数据层，对ODS层进行数据清洗
 */
public interface EventLogDWDService extends ETLService {
    /**
     * 从事件日志中抓取评论内容
     *
     * @param sql
     */
    void comment(String sql);

    /**
     * 从事件日志中抓取点赞内容
     *
     * @param sql
     */
    void praise(String sql);

    /**
     * 从事件日志中抓取活跃用户内容
     *
     * @param sql
     */
    void active(String sql);
}
