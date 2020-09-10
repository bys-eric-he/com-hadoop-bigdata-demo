package com.hadoop.hive.etl.mapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * 读取一年中某天的最高气温
 */
@Slf4j
@Component
public class WeatherMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

    private final Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
            throws IOException {
        // 打印输入样本 如 2018120715
        log.info("==== Before Mapper: ===={}", value);
        String line = value.toString();
        // 截取年份
        String year = line.substring(0, 4);
        // 截取温度
        int temperature = Integer.parseInt(line.substring(8));
        word.set(year);
        output.collect(word, new LongWritable(temperature));

        // 打印输出样本
        log.info("==== After Mapper: ==== {},{}", new Text(year), new LongWritable(temperature));
    }

}
