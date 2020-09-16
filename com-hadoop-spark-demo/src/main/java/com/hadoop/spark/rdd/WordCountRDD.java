package com.hadoop.spark.rdd;

import com.hadoop.spark.common.SpringContextHolder;
import com.hadoop.spark.domain.WordCount;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * 统计单词次数
 */
@Slf4j
@Service
public class WordCountRDD implements Serializable {

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
     * 统计单词出现次数
     *
     * @param inputPath
     * @return
     */
    public List<WordCount> doWordCount(String inputPath) {

        log.info("-----------开始执行WordCountRDD-------------");

        // 获取本地文件 生成javaRDD
        JavaRDD<String> file = threadLocal.get().textFile(inputPath);
        // 按空格分解为数组 生成新的javaRDD
        JavaRDD<String> words = file.flatMap(
                line -> Arrays.asList(line.split(" ")).iterator()
        );
        // 统计每个词出现的次数 生成新的javaRDD
        JavaRDD<WordCount> wordcount = words.map(
                word -> new WordCount(word, 1)
        );
        // 将词与数转换为 key-value形式
        JavaPairRDD<String, Integer> pair = wordcount.mapToPair(
                wordCount -> new Tuple2<>(wordCount.getWord(), wordCount.getCount())
        );
        // 根据key进行整合
        JavaPairRDD<String, Integer> wordcounts = pair.reduceByKey(
                (count1, count2) -> count1 + count2
        );
        // 将结果转换为 WordCount对象
        JavaRDD<WordCount> map = wordcounts.map(
                (tuple2) -> new WordCount(tuple2._1, tuple2._2)
        );
        log.info("----------结束执行WordCountRDD---------------");
        // 将结果转换为 list并返回
        return map.collect();
    }
}
