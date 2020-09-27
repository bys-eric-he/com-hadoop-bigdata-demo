package com.hadoop.hive.service.impl.ods;

import com.hadoop.hive.repository.HiveRepository;
import com.hadoop.hive.service.ods.ODSService;
import org.springframework.beans.factory.annotation.Autowired;

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
        String sql = this.takeContext(t);
        hiveRepository.insertIntoTable(sql);
    }
}
