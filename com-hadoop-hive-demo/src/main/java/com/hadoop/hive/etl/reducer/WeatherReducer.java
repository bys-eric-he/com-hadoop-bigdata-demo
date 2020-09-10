package com.hadoop.hive.etl.reducer;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Iterator;

/**
 * 统计一年天气最高温
 */
@Slf4j
@Component
public class WeatherReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output,
                       Reporter reporter) throws IOException {
        long maxValue = Integer.MIN_VALUE;
        StringBuilder sb = new StringBuilder();

        // 取values温度的最大值
        while (values.hasNext()) {
            long tmp = values.next().get();
            maxValue = Math.max(maxValue, tmp);
            sb.append(tmp).append(", ");

            output.collect(key, new LongWritable(maxValue));
        }

        // 打印输入样本，如 2000， 15 ，99， 12
        log.info("==== Before Reduce ==== {}, {}", key, sb.toString());
        // 打印输出样本
        log.info("==== After Reduce ==== {}, {}", key, sb.toString());
    }
}
