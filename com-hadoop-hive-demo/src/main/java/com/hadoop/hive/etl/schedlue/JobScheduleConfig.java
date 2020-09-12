package com.hadoop.hive.etl.schedlue;

import com.hadoop.hive.etl.job.FriendsJob;
import com.hadoop.hive.etl.job.WeatherJob;
import com.hadoop.hive.etl.job.WordCountJob;
import com.hadoop.hive.service.HadoopHDFSService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Component
public class JobScheduleConfig {
    @Autowired
    private WordCountJob wordCountJob;

    @Autowired
    private FriendsJob friendsJob;

    @Autowired
    private WeatherJob weatherJob;

    @Autowired
    private HadoopHDFSService hadoopHDFSService;

    /**
     * WordCountJob 每30秒触发任务
     */
    @Scheduled(cron = "20 * * * * ?")
    public void jobWordCountExecute() {
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        cachedThreadPool.execute(() -> {
            try {
                log.info("--------------------开始执行 WordCount Job作业----------------");
                hadoopHDFSService.showFiles("/eric");
                wordCountJob.execute();
                log.info("--------------------结束执行 WordCount Job作业----------------");
            } catch (Exception e) {
                e.printStackTrace();
            }
            log.info(Thread.currentThread().getName() + " 线程执行完成!");
        });
    }

    /**
     * WeatherJob 每30秒触发任务
     */
    @Scheduled(cron = "15 * * * * ?")
    public void jobWeatherExecute() {
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        cachedThreadPool.execute(() -> {
            try {
                log.info("--------------------开始执行 WeatherJob Job作业----------------");
                hadoopHDFSService.showFiles("/eric");
                weatherJob.execute();
                log.info("--------------------结束执行 WeatherJob Job作业----------------");
            } catch (Exception e) {
                e.printStackTrace();
            }
            log.info(Thread.currentThread().getName() + " 线程执行完成!");
        });
    }

    /**
     * FriendsJob 每30秒触发任务
     */
    @Scheduled(cron = "10 * * * * ?")
    public void jobFriendsExecute() {
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        cachedThreadPool.execute(() -> {
            try {
                log.info("--------------------开始执行 Friends Job作业----------------");
                hadoopHDFSService.showFiles("/eric");
                friendsJob.execute();
                log.info("--------------------结束执行 Friends Job作业----------------");
            } catch (Exception e) {
                e.printStackTrace();
            }

            log.info(Thread.currentThread().getName() + " 线程执行完成!");
        });
    }
}
