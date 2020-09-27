package com.hadoop.hive.etl.schedlue;

import com.hadoop.hive.common.DateUtil;
import com.hadoop.hive.etl.job.FriendsJob;
import com.hadoop.hive.etl.job.WeatherJob;
import com.hadoop.hive.etl.job.WordCountJob;
import com.hadoop.hive.service.HadoopHDFSService;
import com.hadoop.hive.service.dwd.DWDService;
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

    @Autowired
    private DWDService dwdService;

    private String sqlODSToDWDEvent = String.format("INSERT OVERWRITE TABLE dwd_base_event_log\n" +
                    "PARTITION(dt='%s')\n" +
                    "SELECT\n" +
                    "base_analizer(line,'mid') as mid_id,\n" +
                    "base_analizer(line,'uid') as user_id,\n" +
                    "base_analizer(line,'vc') as version_code,\n" +
                    "base_analizer(line,'vn') as version_name,\n" +
                    "base_analizer(line,'l') as lang,\n" +
                    "base_analizer(line,'sr') as source,\n" +
                    "base_analizer(line,'os') as os,\n" +
                    "base_analizer(line,'ar') as area,\n" +
                    "base_analizer(line,'md') as model,\n" +
                    "base_analizer(line,'ba') as brand,\n" +
                    "base_analizer(line,'sv') as sdk_version,\n" +
                    "base_analizer(line,'g') as gmail,\n" +
                    "base_analizer(line,'hw') as height_width,\n" +
                    "base_analizer(line,'t') as app_time,\n" +
                    "base_analizer(line,'nw') as network,\n" +
                    "base_analizer(line,'ln') as lng,\n" +
                    "base_analizer(line,'la') as lat,\n" +
                    "event_name,\n" +
                    "event_json,\n" +
                    "base_analizer(line,'st') as server_time\n" +
                    "FROM ods_event_log lateral view flat_analizer(base_analizer(line,'et')) tem_flat as event_name,event_json\n" +
                    "WHERE dt='%s'\n" +
                    "AND base_analizer(line,'et')<>''",
            DateUtil.getCurrentDate("yyyy-MM-dd"),
            DateUtil.getCurrentDate("yyyy-MM-dd"));

    private String sqlODSToDWDStart = String.format("INSERT OVERWRITE TABLE dwd_start_log\n" +
                    "PARTITION (dt='%s')\n" +
                    "SELECT\n" +
                    "get_json_object(line,'$.mid') mid_id,\n" +
                    "get_json_object(line,'$.uid') user_id,\n" +
                    "get_json_object(line,'$.vc') version_code,\n" +
                    "get_json_object(line,'$.vn') version_name,\n" +
                    "get_json_object(line,'$.l') lang,\n" +
                    "get_json_object(line,'$.sr') source,\n" +
                    "get_json_object(line,'$.os') os,\n" +
                    "get_json_object(line,'$.ar') area,\n" +
                    "get_json_object(line,'$.md') model,\n" +
                    "get_json_object(line,'$.ba') brand,\n" +
                    "get_json_object(line,'$.sv') sdk_version,\n" +
                    "get_json_object(line,'$.g') gmail,\n" +
                    "get_json_object(line,'$.hw') height_width,\n" +
                    "get_json_object(line,'$.t') app_time,\n" +
                    "get_json_object(line,'$.nw') network,\n" +
                    "get_json_object(line,'$.ln') lng,\n" +
                    "get_json_object(line,'$.la') lat,\n" +
                    "get_json_object(line,'$.entry') entry,\n" +
                    "get_json_object(line,'$.open_ad_type') open_ad_type,\n" +
                    "get_json_object(line,'$.action') action,\n" +
                    "get_json_object(line,'$.loading_time') loading_time,\n" +
                    "get_json_object(line,'$.detail') detail,\n" +
                    "get_json_object(line,'$.extend1') extend1 FROM ods_start_log WHERE dt='%s'",
            DateUtil.getCurrentDate("yyyy-MM-dd"),
            DateUtil.getCurrentDate("yyyy-MM-dd"));

    /**
     * 需要执行./hadoop fs -chmod 777 /warehouse/gmall/dwd/dwd_start_log 对hive用户进行授权
     * 否则会报 FAILED: RuntimeException Cannot create staging directory 'hdfs://e071096005c4:9000/warehouse/gmall/dwd/dwd_start_log/dt=2020-09-27/.hive-staging_hive_2020-09-27_03-43-57_035_2148767034042485060-5': Permission denied: user=hive, access=WRITE, inode="/warehouse/gmall/dwd/dwd_start_log/dt=2020-09-27/.hive-staging_hive_2020-09-27_03-43-57_035_2148767034042485060-5":root:supergroup:drwxr-xr-x
     */
    //@Scheduled(cron = "0/20 * * * * ?")
    public void jobStartLogODSToDWDExecute() {
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        cachedThreadPool.execute(() -> {
            try {
                log.info("--------------------开始执行 StartLog ODS To DWD Job作业----------------");
                dwdService.execute(sqlODSToDWDStart);
                log.info("--------------------结束执行 StartLog ODS To DWD Job作业----------------");
            } catch (Exception e) {
                e.printStackTrace();
            }
            log.info(Thread.currentThread().getName() + " 线程执行完成!");
        });
    }

    //@Scheduled(cron = "0/20 * * * * ?")
    public void jobEventLogODSToDWDExecute() {
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        cachedThreadPool.execute(() -> {
            try {
                log.info("--------------------开始执行 EventLog ODS To DWD Job作业----------------");
                dwdService.execute(sqlODSToDWDEvent);
                log.info("--------------------结束执行 EventLog ODS To DWD Job作业----------------");
            } catch (Exception e) {
                e.printStackTrace();
            }
            log.info(Thread.currentThread().getName() + " 线程执行完成!");
        });
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
