package com.hadoop.spark.rdd;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LogRDD {

    @Autowired
    private JavaSparkContext javaSparkContext;

    /**
     * 统计Error日志个数
     *
     * @param path 日志文件路径
     */
    public void errorLogCounts(String path) {
        JavaRDD<String> inputRDD = javaSparkContext.textFile(path);
        JavaRDD<String> errorsRDD = inputRDD.filter(s -> s.contains("ERROR"));
        log.info("-->errors显示为：" + errorsRDD.collect());
        log.info("-->errors个数为：" + errorsRDD.count());
    }
}
