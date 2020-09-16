package com.hadoop.spark.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.mortbay.util.ajax.JSON;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

@Slf4j
@Component
public class WordCountJob extends AbstractSparkJob {

    @Override
    protected void execute(JavaSparkContext sparkContext, String[] args) {
        //args[1] 文件路径, 读取文件wordcount后输出
        List<Tuple2<Integer, String>> topK = sparkContext.textFile(args[1])
                .flatMap(str -> Arrays.asList(str.split("\n| ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((integer1, integer2) -> integer1 + integer2)
                .filter(tuple2 -> tuple2._1.length() > 0)
                //单词与频数倒过来为新二元组，按频数倒排序取途topK
                .mapToPair(tuple2 -> new Tuple2<>(tuple2._2, tuple2._1))
                .sortByKey(false)
                .take(10);
        for (Tuple2<Integer, String> tuple2 : topK) {
            log.info("--Word-->{}",JSON.toString(tuple2));
        }

        //将结果保存到文本文件
        sparkContext.parallelize(topK).coalesce(1).saveAsTextFile(args[2]);
    }
}
