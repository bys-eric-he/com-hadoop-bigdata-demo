package com.hadoop.hive.service.impl;

import com.hadoop.hive.entity.user.UserAccessTimes;
import com.hadoop.hive.repository.UserAccessTimesRepository;
import com.hadoop.hive.service.UserAccessTimesService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class UserAccessTimesServiceImpl implements UserAccessTimesService {

    @Autowired
    private UserAccessTimesRepository repository;

    /**
     * 获取用户访问次数原始数据列表
     *
     * @return
     */
    @Override
    public List<UserAccessTimes> getUserAccessTimesList() {
        String sql = "select username,month,counts from user_access_times";
        return repository.getUserAccessTimesList(sql);
    }

    /**
     * 按用户和月份统计访问次数
     *
     * @return
     */
    @Override
    public List<UserAccessTimes> getUserAccessTimeGroupByNameAndMonth() {
        String sql = "select  username,substr(month,1,7) as month,sum(counts) from user_access_times group by username,substr(month,1,7)";
        return repository.getUserAccessTimesList(sql);
    }
}
