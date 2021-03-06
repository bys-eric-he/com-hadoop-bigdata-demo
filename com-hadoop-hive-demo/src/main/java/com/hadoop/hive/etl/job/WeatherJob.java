package com.hadoop.hive.etl.job;

import com.hadoop.hive.etl.mapper.WeatherMapper;
import com.hadoop.hive.etl.reducer.WeatherReducer;
import com.hadoop.hive.service.HadoopHDFSService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

/**
 * 一年最高气温统计
 */
@Slf4j
public class WeatherJob {
    private String jobName;
    private String inputPath;
    private String outputPath;

    @Autowired
    private HadoopHDFSService hadoopHDFSService;

    public WeatherJob(String jobName, String inputPath, String outputPath) {
        this.jobName = jobName;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    /**
     * 获取单词一年最高气温计算配置
     *
     * @param jobName
     * @return
     */
    public JobConf getWeatherJobsConf(String jobName) {
        //JobConf代表一个Map/Reduce作业的配置。
        //是用户向Hadoop框架描述一个Map/Reduce作业如何执行的主要接口，框架会按照JobConf描述的信息忠实地去尝试完成这个作业。
        JobConf jobConf = new JobConf(hadoopHDFSService.getConfiguration());
        jobConf.setJobName(jobName);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(LongWritable.class);
        jobConf.setMapperClass(WeatherMapper.class);
        jobConf.setReducerClass(WeatherReducer.class);
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);
        return jobConf;
    }

    /**
     * 统计数据文件的共同好友
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void execute() throws Exception {
        if (StringUtils.isEmpty(jobName) || StringUtils.isEmpty(inputPath)) {
            return;
        }
        // 输出目录 = output/当前Job
        if (hadoopHDFSService.existDir(outputPath)) {
            hadoopHDFSService.deleteDir(outputPath);
        }
        JobConf jobConf = getWeatherJobsConf(jobName);
        FileInputFormat.setInputPaths(jobConf, new Path(inputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));
        log.info("--->开始执行 WeatherJob......");
        //runJob(JobConf job)阻塞（同步）方式提交MapReduce作业后立即返回
        //submitJob(JobConf job)非阻塞（异步）方式提交MapReduce作业后立即返回
        JobClient.runJob(jobConf);
        log.info("----Weather Job Finished----");
    }
}
