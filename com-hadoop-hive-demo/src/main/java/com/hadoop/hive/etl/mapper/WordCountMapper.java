package com.hadoop.hive.etl.mapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * 继承自Mapper类，负责重写父类Mapper中的map方法
 * 泛型参数LongWritable表示文本偏移量相当于读取文本行的地址，由MapReduce框架启动时自动根据文件获取
 * 泛型参数Text表示读取的一行文本
 * 泛型参数Text表示map方法输出key的类型
 * 泛型参数IntWritable表示map方法输出的value类型
 */
@Slf4j
@Component
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    //定义一个常量，并将值初始化为1
    private static final IntWritable one = new IntWritable(1);

    //定义一个静态Text类的引用为word
    private static final Text word = new Text();

    /**
     * 主要负责对文本文件内容进行映射处理，把一行文本切分成一个个单词，并映射为一对对key-value键值对之后输出。
     *
     * @param key     从文件中读取的每行文本的偏移地址
     * @param value   从文件中读取的一行文本，由MapReduce框架负责传入
     * @param context MapReduce框架的上下文对象，可以存放公共类型的数据，比如map方法处理完的中间结果可以保存到context上下文对象中,
     *                MapReduce框架再根据上下文对象中的数据将其持久化到本地磁盘，这都是MapReduce框架来完成的
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //将读取的一行文本转化为Java的字符串类型
        String line = value.toString();
        log.info("----------读取文件行内容------------\n{}", line);
        //按照空格符切分出一行字符串中包含的所有单词，并存储到字符串数组中
        String[] words = line.split(" ");

        for (String w : words) {
            word.set(w);
            //经过处理形成key-value键值对,输出到MapReduce的上下文，由MapReduce的上下文将结果写入本地磁盘空间
            context.write(word, one);
            log.info("----------- word:{} one:{} ----------", word.toString(), one.get());
        }
        log.info("-----------WordCountMapper->map方法执行完毕----------");
    }
}
