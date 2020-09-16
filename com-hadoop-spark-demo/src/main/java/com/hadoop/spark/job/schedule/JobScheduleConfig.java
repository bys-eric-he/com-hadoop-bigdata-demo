package com.hadoop.spark.job.schedule;

import com.hadoop.spark.common.SpringContextHolder;
import com.hadoop.spark.domain.WordCount;
import com.hadoop.spark.job.AbstractSparkJob;
import com.hadoop.spark.rdd.ActionRDD;
import com.hadoop.spark.rdd.LogRDD;
import com.hadoop.spark.rdd.WordCountRDD;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.util.Utils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Component
public class JobScheduleConfig {
    @Autowired
    private WordCountRDD wordCountRDD;

    @Autowired
    private LogRDD logRDD;

    @Autowired
    private ActionRDD actionRDD;

    /**
     * 统计单词出现次数
     */
    @Scheduled(cron = "0/30 * * * * ?")
    public void wordCountRDDJob() {
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        cachedThreadPool.execute(() -> {
            List<WordCount> wordCounts = wordCountRDD.doWordCount("/home/eric/GitHubProject/com-hadoop-bigdata-demo/log/hive_log/log_error.log");

            for (WordCount wordCount : wordCounts) {
                log.info("-->单词:{}, -->次数:{}", wordCount.getWord(), wordCount.getCount());
            }
        });
    }

    /**
     * 统计ERROR日志发生次数
     */
    @Scheduled(cron = "0/25 * * * * ?")
    public void errorLogJob() {
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        cachedThreadPool.execute(() -> {
            logRDD.errorLogCounts("/home/eric/GitHubProject/com-hadoop-bigdata-demo/log/hive_log/log_error.log");
        });
    }

    /**
     * RDD操作Demo
     */
    @Scheduled(cron = "0/20 * * * * ?")
    public void actionAddJob() {
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        cachedThreadPool.execute(() -> {
            actionRDD.reduce();
            actionRDD.collect();
            actionRDD.count();
            actionRDD.first();
            actionRDD.countByKey();
            actionRDD.forEach();
            actionRDD.take(10);
        });
    }

    /**
     * 统计单词出现次数
     */
    @Scheduled(cron = "0/15 * * * * ?")
    public void wordCountSparkJob() {
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        cachedThreadPool.execute(() -> {
            String[] args = {"com.hadoop.spark.job.WordCountJob", "/home/eric/data/spark_data/input/word_data.txt", "/home/eric/data/spark_data/output"};
            Object sparkJob = SpringContextHolder.getBean(Utils.classForName(args[0]));
            if (sparkJob instanceof AbstractSparkJob) {
                ((AbstractSparkJob) sparkJob).startJob(args);
            } else {
                log.error("--->你指定的启动job类" + args[0] + "不存在!");
            }
        });
    }

    /**
     * 统计单词出现次数
     */
    @Scheduled(cron = "0/5 * * * * ?")
    public void wordCountSocketJob() {
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        cachedThreadPool.execute(() -> {
            String[] args = {"com.hadoop.spark.job.streaming.WordCountSocketJob", "host", "9999"};
            Object sparkJob = SpringContextHolder.getBean(Utils.classForName(args[0]));
            if (sparkJob instanceof AbstractSparkJob) {
                ((AbstractSparkJob) sparkJob).startJob(args);
            } else {
                log.error("--->你指定的启动job类" + args[0] + "不存在!");
            }
        });
    }
}
