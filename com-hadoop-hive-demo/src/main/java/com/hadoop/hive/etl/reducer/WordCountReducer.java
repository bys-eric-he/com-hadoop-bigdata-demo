package com.hadoop.hive.etl.reducer;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * 继承自Reducer类，负责重写父类Reducer中的reduce方法
 * 参数第一个Text表示reduce函数输入的键值对的键值类型
 * 参数第一个IntWritable表示reduce函数输入的键值对的值类型
 * 参数第二个Text表示reduce函数的键类型
 * 参数第二个IntWritable表示reduce函数输出键值对的值类型
 */
@Slf4j
@Component
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * reduce函数主要负责对map函数处理之后的中间结果进行最后处理，负责聚合统计，将map函数处理完的中间结果key-value键和值的列表
     * 针对每个key对应的value值列表集合中的数值，进行聚合累加计算。
     * 参数key是map函数处理完后输出的中间结果键值对的键值
     * values是map函数处理完后输出的中间结果值的列表
     * context上MapReduce框架的上下文对象，可以存放公共类型的数据，比如reduce函数处理完成的中间结果可以保存到context上下文对象中
     * 由上下文再写入HDFS中
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //初始化一个局部int型变量值为0，统计最终每个单词出现的次数
        int sum = 0;

        for (IntWritable v : values) {
            sum += v.get();
            log.info("******* 单词:{} 次数:{} *******", key, v.get());
        }
        //将reduce处理完的结果输出到HDFS文件系统中
        context.write(key, new IntWritable(sum));

        log.info("-----------单词:{} 总计次数:{}----------", key, sum);
    }
}
