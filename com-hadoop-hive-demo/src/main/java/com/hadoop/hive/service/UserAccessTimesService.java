package com.hadoop.hive.service;

import com.hadoop.hive.entity.user.UserAccessTimes;

import java.util.List;

public interface UserAccessTimesService {

    /**
     * 获取用户访问次数原始数据列表
     *
     * @return
     */
    List<UserAccessTimes> getUserAccessTimesList();

    /**
     * 按用户和月份统计访问次数
     *
     * @return
     */
    List<UserAccessTimes> getUserAccessTimeGroupByNameAndMonth();
}
