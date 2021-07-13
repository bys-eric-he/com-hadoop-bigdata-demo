package com.hadoop.flink.operator;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

/**
 * 通用的FlatMap操作Function
 */
@Slf4j
public class SplitterFlatMapOperator implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

        if (StringUtils.isNullOrWhitespaceOnly(s)) {
            log.warn("----invalid line----");
            return;
        }

        for (String word : s.split(" ")) {
            collector.collect(new Tuple2<String, Integer>(word, 1));
        }
    }
}
