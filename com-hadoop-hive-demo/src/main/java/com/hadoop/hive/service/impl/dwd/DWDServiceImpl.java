package com.hadoop.hive.service.impl.dwd;

import com.hadoop.hive.repository.HiveRepository;
import com.hadoop.hive.service.dwd.DWDService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DWDServiceImpl implements DWDService {
    @Autowired
    private HiveRepository hiveRepository;

    /**
     * 执行统计语句
     *
     * @param sql
     */
    @Override
    public void execute(String sql) {
        hiveRepository.execute(sql);
    }
}
