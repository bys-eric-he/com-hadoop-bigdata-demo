package com.hadoop.spark.rdd;

import com.hadoop.spark.common.SpringContextHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Slf4j
@Component
public class LogRDD implements Serializable {
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

    /**
     * 统计Error日志个数
     *
     * @param path 日志文件路径
     */
    public void errorLogCounts(String path) {
        log.info("-----------開始 執行统计ERROR日志发生次数------------");
        JavaRDD<String> inputRDD = threadLocal.get().textFile(path);
        JavaRDD<String> errorsRDD = inputRDD.filter(s -> s.contains("ERROR"));
        log.info("-->errors显示为：" + errorsRDD.collect());
        log.info("-->errors个数为：" + errorsRDD.count());
        log.info("-----------結束 執行统计ERROR日志发生次数------------");
    }
}
