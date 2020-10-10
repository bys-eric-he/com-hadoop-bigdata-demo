package com.hadoop.hive.service.ads;

import com.hadoop.hive.service.ETLService;

/**
 * ADS层 数据应用层，为各种报表提供可视化数据
 */
public interface StartLogADSService extends ETLService {
    /**
     * 活跃设备数
     *
     * @param sql
     */
    void activeDevices(String sql);

    /**
     * 连续活跃设备数
     *
     * @param sql
     */
    void continuousActiveDevices(String sql);

    /**
     * 最近连续三周活跃用户数
     *
     * @param sql
     */
    void threeConsecutiveWeeks(String sql);

    /**
     * 每日用户留存情况
     *
     * @param sql
     */
    void dailyUserRetentionStatus(String sql);

    /**
     * 流失用户数
     *
     * @param sql
     */
    void lostUsers(String sql);

    /**
     * 每日新增设备信息数量
     *
     * @param sql
     */
    void newDeviceAddedDaily(String sql);

    /**
     * 沉默用户数
     *
     * @param sql
     */
    void numberOfSilentUsers(String sql);

    /**
     * 本周回流用户数
     *
     * @param sql
     */
    void returningUsers(String sql);
}
