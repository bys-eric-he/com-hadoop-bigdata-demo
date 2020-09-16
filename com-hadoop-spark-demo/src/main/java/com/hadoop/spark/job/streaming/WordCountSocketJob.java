package com.hadoop.spark.job.streaming;

import com.hadoop.spark.job.AbstractSparkJob;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Spark Streaming 是Spark API核心的扩展，支持实时数据流的可扩展，高吞吐量，容错流处理。
 * 数据可以从Kafka，Flume， Kinesis， 或TCP Socket来源获得，
 * 并且可以使用与高级别功能表达复杂的算法来处理map，reduce，join和window。
 * 最后，处理后的数据可以推送到文件系统，数据库和实时仪表看板。
 */
@Component
public class WordCountSocketJob extends AbstractSparkJob {

    @Autowired
    private SparkConf sparkConf;

    @Override
    protected void execute(JavaSparkContext sparkContext, String[] args) {
        //获得JavaStreamingContext
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));

        //从socket源获取数据
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(args[1], Integer.parseInt(args[2]));

        //拆分行成单词
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        //计算每个单词出现的个数
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        //输出结果
        wordCounts.print();

        //开始作业
        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ssc.close();
        }
    }
}
