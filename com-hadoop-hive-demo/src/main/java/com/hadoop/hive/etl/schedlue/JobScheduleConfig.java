package com.hadoop.hive.etl.schedlue;

import com.hadoop.hive.constant.HiveSQL;
import com.hadoop.hive.etl.job.FriendsJob;
import com.hadoop.hive.etl.job.WeatherJob;
import com.hadoop.hive.etl.job.WordCountJob;
import com.hadoop.hive.service.HadoopHDFSService;
import com.hadoop.hive.service.ads.StartLogADSService;
import com.hadoop.hive.service.dwd.StartLogDWDService;
import com.hadoop.hive.service.dws.StartLogDWSService;
import com.hadoop.hive.service.dwt.StartLogDWTService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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

    @Autowired
    @Qualifier("startLogDWDService")
    private StartLogDWDService dwdService;

    @Autowired
    @Qualifier("startLogDWSService")
    private StartLogDWSService dwsService;

    @Autowired
    @Qualifier("startLogDWTService")
    private StartLogDWTService dwtService;

    @Autowired
    @Qualifier("startLogADSService")
    private StartLogADSService adsService;

    /**
     * 需要执行./hadoop fs -chmod 777 /warehouse/gmall/dwd/dwd_start_log 对hive用户进行授权
     * 否则会报 FAILED: RuntimeException Cannot create staging directory 'hdfs://e071096005c4:9000/warehouse/gmall/dwd/dwd_start_log/dt=2020-09-27/.hive-staging_hive_2020-09-27_03-43-57_035_2148767034042485060-5': Permission denied: user=hive, access=WRITE, inode="/warehouse/gmall/dwd/dwd_start_log/dt=2020-09-27/.hive-staging_hive_2020-09-27_03-43-57_035_2148767034042485060-5":root:supergroup:drwxr-xr-x
     * 每天11:19点执行
     */
    //@Scheduled(cron = "0 30 11 * * ?")
    public void jobStartLogODSToDWDExecute() {
        try {
            dwdService.execute(HiveSQL.SQL_ODS_TO_DWD_START);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 每天12:00执行
     */
    //@Scheduled(cron = "0 0 12 * * ?")
    public void jobEventLogODSToDWDExecute() {
        try {
            dwdService.execute(HiveSQL.SQL_ODS_TO_DWD_EVENT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 每天12:30执行
     */
    //@Scheduled(cron = "0 30 12 * * ?")
    public void jobStartLogDWDToDWSExecute() {
        try {
            dwsService.execute(HiveSQL.SQL_DWD_TO_DWS_START);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 每天13:00执行
     */
    //@Scheduled(cron = "0 0 13 * * ?")
    public void jobStartLogDWSToDWTExecute() {
        try {
            dwsService.execute(HiveSQL.SQL_DWS_TO_DWT_START);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * WordCountJob 每30秒触发任务
     */
    //@Scheduled(cron = "0/20 * * * * ?")
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
    //@Scheduled(cron = "0/15 * * * * ?")
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
    //@Scheduled(cron = "0/10 * * * * ? ")
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
