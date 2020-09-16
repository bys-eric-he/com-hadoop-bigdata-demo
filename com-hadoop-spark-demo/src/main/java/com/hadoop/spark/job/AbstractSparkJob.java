package com.hadoop.spark.job;

import com.hadoop.spark.common.SpringContextHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * 抽象job
 */
@Slf4j
public abstract class AbstractSparkJob implements Serializable {

    /**
     * java.io.NotSerializableException: org.apache.spark.api.java.JavaSparkContext
     * sparkcontext无法被序列化的问题 当我们在使用RDD调用map等算子，或者Dstream使用transform时，
     * 我们需要在它们的重写的方法里面，需要利用sparkcontext
     * 比如把一个集合转化为RDD，但是一运行就报java.io.NotSerializableException:
     * org.apache.spark.api.java.JavaSparkContext（sparkcontext序列化异常）
     * <p>
     * 因为它是不能序列化的，这时候我们使用static修飾解決該問題
     */
    private static ThreadLocal<JavaSparkContext> threadLocal = new ThreadLocal<JavaSparkContext>() {
        protected JavaSparkContext initialValue() {
            return SpringContextHolder.getBean(JavaSparkContext.class);
        }
    };

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
        this.execute(threadLocal.get(), args);
        //不能關閉
        //this.close(sparkContext);
        log.info("-------结束执行Job-----------");
    }

}
