package com.hadoop.hive.service.ods.impl;

import com.hadoop.hive.repository.HiveRepository;
import com.hadoop.hive.service.ods.ODSService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public abstract class AbstractODSService<T> implements ODSService<T> {

    @Autowired
    private HiveRepository hiveRepository;

    protected abstract String takeContext(T t);

    /**
     * 保存日志包原始数据
     *
     * @param t
     */
    @Override
    public void insert(T t) {
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        cachedThreadPool.execute(() -> {
            try {
                log.info("---------------接收到原始数据--------------");
                String sql = this.takeContext(t);
                hiveRepository.insertIntoTable(sql);
                log.info("---------------原始数据处理结束--------------");
            } catch (Exception e) {
                log.error("******执行原始数据作业异常->{}", e.getMessage());
                e.printStackTrace();
            }
        });
    }
}
