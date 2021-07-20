package com.hadoop.flink.operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Locale;

/**
 * FlatMap: 一行变零到多行。如下，将一个句子(一行)分割成多个单词(多行)。
 */
public class DataStreamFlatMapOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 输入: 英文电影台词
        DataStreamSource<String> source = env
                .fromElements(
                        "You jump jump jump jump I jump",
                        "You jump I jump",
                        "Life was like a box of chocolates !"
                );

        // 转换: 将包含chocolates的句子转换为每行一个单词
        SingleOutputStreamOperator<String> result = source.flatMap(new MyFlatMapFunction());

        // 输出: 输出到控制台
        // Life
        // was
        // like
        // a
        // box
        // of
        // chocolates
        result.print("--------FlatMap--------");

        KeyedStream<Tuple2<String, Integer>, String> counts = source
                .flatMap(new WordCountFlatMapFunction())
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.getField(0);
                    }
                });

        // 输出: 输出到控制台
        // Life 1
        // was 1
        // like 1
        // a 1
        // box 1
        // of 1
        // chocolates 1
        counts.sum("f1")
                .print("-----FlatMap Counts-----")
                .setParallelism(1);
        env.execute();
    }

    /**
     * 单词切割
     */
    public static class MyFlatMapFunction implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            if (value.contains("chocolates")) {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        }
    }

    /**
     * 统计单词出现次数
     */
    public static class WordCountFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.toLowerCase(Locale.ROOT).split(" ");
            for (String word : words) {
                if (word.length() > 0) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}
