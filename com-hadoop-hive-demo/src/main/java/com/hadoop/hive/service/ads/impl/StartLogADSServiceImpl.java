package com.hadoop.hive.service.ads.impl;

import com.hadoop.hive.repository.HiveRepository;
import com.hadoop.hive.service.ads.StartLogADSService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service("startLogADSService")
public class StartLogADSServiceImpl implements StartLogADSService {
    @Autowired
    private HiveRepository hiveRepository;

    /**
     * 执行统计语句
     *
     * @param sql
     */
    @Override
    public void execute(String sql) {

    }

    /**
     * 活跃设备数
     *
     * @param sql
     */
    @Override
    public void activeDevices(String sql) {
        try {
            log.info("--------------------开始执行 StartLog Active Devices DWT To ADS Job作业----------------");
            hiveRepository.execute(sql);
            log.info("--------------------结束执行 StartLog Active Devices DWT To ADS Job作业----------------");
        } catch (Exception e) {
            log.error("******执行作业异常->{}", e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 连续活跃设备数
     *
     * @param sql
     */
    @Override
    public void continuousActiveDevices(String sql) {
        try {
            log.info("--------------------开始执行 StartLog Continuous Active Devices DWT To ADS Job作业----------------");
            hiveRepository.execute(sql);
            log.info("--------------------结束执行 StartLog Continuous Active Devices DWT To ADS Job作业----------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 最近连续三周活跃用户数
     *
     * @param sql
     */
    @Override
    public void threeConsecutiveWeeks(String sql) {
        try {
            log.info("--------------------开始执行 StartLog Three Consecutive Weeks DWT To ADS Job作业----------------");
            hiveRepository.execute(sql);
            log.info("--------------------结束执行 StartLog Three Consecutive Weeks DWT To ADS Job作业----------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 每日用户留存情况
     *
     * @param sql
     */
    @Override
    public void dailyUserRetentionStatus(String sql) {
        try {
            log.info("--------------------开始执行 StartLog Daily User Retention Status DWT To ADS Job作业----------------");
            hiveRepository.execute(sql);
            log.info("--------------------结束执行 StartLog Daily User Retention Status DWT To ADS Job作业----------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 流失用户数
     *
     * @param sql
     */
    @Override
    public void lostUsers(String sql) {
        try {
            log.info("--------------------开始执行 StartLog Lost User DWT To ADS Job作业----------------");
            hiveRepository.execute(sql);
            log.info("--------------------结束执行 StartLog Lost User DWT To ADS Job作业----------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 每日新增设备信息数量
     *
     * @param sql
     */
    @Override
    public void newDeviceAddedDaily(String sql) {
        try {
            log.info("--------------------开始执行 StartLog New Device Added Daily DWT To ADS Job作业----------------");
            hiveRepository.execute(sql);
            log.info("--------------------结束执行 StartLog New Device Added Daily DWT To ADS Job作业----------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 沉默用户数
     *
     * @param sql
     */
    @Override
    public void numberOfSilentUsers(String sql) {
        try {
            log.info("--------------------开始执行 StartLog Number Of Silent Users DWT To ADS Job作业----------------");
            hiveRepository.execute(sql);
            log.info("--------------------结束执行 StartLog Number Of Silent Users DWT To ADS Job作业----------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 本周回流用户数
     *
     * @param sql
     */
    @Override
    public void returningUsers(String sql) {
        try {
            log.info("--------------------开始执行 StartLog Returning Users DWT To ADS Job作业----------------");
            hiveRepository.execute(sql);
            log.info("--------------------结束执行 StartLog Returning Users DWT To ADS Job作业----------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
