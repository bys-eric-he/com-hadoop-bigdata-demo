package com.hadoop.hive.etl.job;

import com.hadoop.hive.etl.mapper.FriendsMapper;
import com.hadoop.hive.etl.reducer.FriendsReducer;
import com.hadoop.hive.service.HadoopHDFSService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

/**
 * Job作业,用来把map和reduce函数组织起来的组件
 */
@Slf4j
public class FriendsJob {
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
    public FriendsJob(String jobName, String inputPath, String outputPath) {
        this.jobName = jobName;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
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
        Job job = Job.getInstance(hadoopHDFSService.getConfiguration(), jobName);
        // 设置jar中的启动类，可以根据这个类找到相应的jar包
        job.setJarByClass(FriendsMapper.class);
        job.setMapperClass(FriendsMapper.class);
        job.setReducerClass(FriendsReducer.class);

        // 一般情况下mapper和reducer的输出的数据类型是一样的，所以我们用上面两条命令就行，如果不一样，我们就可以用下面两条命令单独指定mapper的输出key、value的数据类型
        // 设置Mapper的输出
        // job.setMapOutputKeyClass(Text.class);
        // job.setMapOutputValueClass(Text.class);

        // 设置reduce的输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 指定输入输出文件的位置
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        log.info("--->开始执行 FriendsJob......");
        job.waitForCompletion(true);
        log.info("----Friends Job Finished----");

    }
}
