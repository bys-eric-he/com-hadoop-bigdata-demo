package com.hadoop.hive.etl.job;

import com.hadoop.hive.etl.mapper.WordCountMapper;
import com.hadoop.hive.etl.reducer.WordCountReducer;
import com.hadoop.hive.service.HadoopHDFSService;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Job作业,用来把map和reduce函数组织起来的组件
 */
@Slf4j
public class WordCountJob {

    private String jobName;
    private String inputPath;
    private String outputPath;

    @Autowired
    private HadoopHDFSService hadoopHDFSService;

    /**
     * Job作业
     *
     * @param jobName    作业名称
     * @param inputPath  源文件输入路径
     * @param outputPath 经过MapReduce数据处理之后结果的输出文件路径，该路径不能事先存在。
     *                   如果事先存在会抛org.apache.hadoop.mapred.FileAlreadyExistsException:
     *                   Output directory hdfs://10.1.2.168:9000/eric/hadoop_data/output already exists异常
     */
    public WordCountJob(String jobName, String inputPath, String outputPath) {
        this.jobName = jobName;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    /**
     * Job作业执行方法
     *
     * @throws Exception
     */
    public void execute() throws Exception {
        //如果输出目录已经存在,则删除目录,该路径不能事先存在。
        if (hadoopHDFSService.existDir(outputPath)) {
            hadoopHDFSService.deleteDir(outputPath);
        }
        //创建MapReduce的job对象，并设置job的名称。
        Job job = Job.getInstance(hadoopHDFSService.getConfiguration(), jobName);
        //设置job运行的程序入口主类
        job.setJarByClass(WordCountJob.class);
        //设置job输入\输出格式为文本格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置job map函数及reduce函数的实现对象
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //设置job map函数执行中间结果输出的key类型
        job.setMapOutputKeyClass(Text.class);
        //设置job map函数执行中间结果输出的value类型
        job.setMapOutputValueClass(IntWritable.class);

        //设置job输出的key类型
        job.setOutputKeyClass(Text.class);
        //设置job输出的value类型
        job.setOutputValueClass(IntWritable.class);

        //设置job输入文件的路径
        FileInputFormat.addInputPath(job, new Path(inputPath));

        //设置job计算结果的输出路径
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        log.info("-->开始执行 WordCount Job......");
        //参数true表示将运行进度等信息及时输出给用户，false的话只是等待作业结束
        job.waitForCompletion(true);
    }
}
