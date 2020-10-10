package com.hadoop.hive.service.dwt.impl;

import com.hadoop.hive.repository.HiveRepository;
import com.hadoop.hive.service.dwt.StartLogDWTService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service("startLogDWTService")
public class StartLogDWTServiceImpl implements StartLogDWTService {
    @Autowired
    private HiveRepository hiveRepository;

    /**
     * 执行统计语句
     *
     * @param sql
     */
    @Override
    public void execute(String sql) {
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        cachedThreadPool.execute(() -> {
            try {
                log.info("--------------------开始执行 >Start Log DWS To DWT Job 作业----------------");
                hiveRepository.execute(sql);
                log.info("--------------------结束执行 >Start Log DWS To DWT Job 作业----------------");
            } catch (Exception e) {
                log.error("******执行作业异常->{}", e.getMessage());
                e.printStackTrace();
            }
        });
    }
}
