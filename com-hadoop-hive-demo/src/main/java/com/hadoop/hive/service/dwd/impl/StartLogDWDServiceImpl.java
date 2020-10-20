package com.hadoop.hive.service.dwd.impl;

import com.hadoop.hive.repository.HiveRepository;
import com.hadoop.hive.service.dwd.StartLogDWDService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service("startLogDWDService")
public class StartLogDWDServiceImpl implements StartLogDWDService {
    @Autowired
    private HiveRepository hiveRepository;

    /**
     * 执行统计语句
     *
     * @param sql
     */
    @Override
    public void execute(String sql) {
        try {
            log.info("--------------------开始执行 StartLog ODS To DWD Job作业----------------");
            hiveRepository.execute(sql);
            log.info("--------------------结束执行 StartLog ODS To DWD Job作业----------------");
        } catch (Exception e) {
            log.error("******执行作业异常->{}", e.getMessage());
            e.printStackTrace();
        }
    }
}
