package com.hadoop.spark.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.Serializable;

/**
 * 抽象job
 */
@Slf4j
public abstract class AbstractSparkJob implements Serializable {
    @Autowired
    private JavaSparkContext sparkContext;

    protected abstract void execute(JavaSparkContext sparkContext, String[] args);

    protected void close(JavaSparkContext javaSparkContext) {
        javaSparkContext.close();
    }

    /**
     * 开始执行Spark Job
     *
     * @param args
     */
    public void startJob(String[] args) {
        log.info("-------开始执行Job-----------");
        this.execute(sparkContext, args);
        this.close(sparkContext);
        log.info("-------结束执行Job-----------");
    }

}
