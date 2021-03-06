package com.hadoop.flink.operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * FlatMap: 一行变零到多行。如下，将一个句子(一行)分割成多个单词(多行)。
 */
public class DataStreamFlatMapOperator {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 输入: 英文电影台词
        DataStreamSource<String> source = env
                .fromElements(
                        "You jump I jump",
                        "Life was like a box of chocolates"
                );

        // 转换: 将包含chocolates的句子转换为每行一个单词
        SingleOutputStreamOperator<String> result = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                if(value.contains("chocolates")){
                    String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(word);
                    }
                }
            }
        });

        // 输出: 输出到控制台
        // Life
        // was
        // like
        // a
        // box
        // of
        // chocolates
        result.print("--------FlatMap--------");

        env.execute();
    }
}
