/*--------------------ADS层 数据应用层，为各种报表提供可视化数据--------------*/

DROP TABLE
    IF EXISTS ads_uv_count;

CREATE external TABLE ads_uv_count
(
    `dt`          string COMMENT '统计日期',
    `day_count`   BIGINT COMMENT '当日用户数量',
    `wk_count`    BIGINT COMMENT '当周用户数量',
    `mn_count`    BIGINT COMMENT '当月用户数量',
    `is_weekend`  string COMMENT 'Y,N 是否是周末,用于得到本周最终结果',
    `is_monthend` string COMMENT 'Y,N 是否是月末,用于得到本月最终结果'
)
    COMMENT '活跃设备数'
    ROW format delimited FIELDS TERMINATED BY '\t'
    location '/warehouse/gmall/ads/ads_uv_count/';


DROP TABLE
    IF EXISTS ads_uv_count;

CREATE external TABLE ads_uv_count
(
    `dt`          string COMMENT 'Statistic Date',
    `day_count`   BIGINT COMMENT 'Number of users of the day',
    `wk_count`    BIGINT COMMENT 'Number of users of the week',
    `mn_count`    BIGINT COMMENT 'Number of users in the month',
    `is_weekend`  string COMMENT 'Y, N is it a weekend, used to get the final result of the week',
    `is_monthend` string COMMENT 'Y, N is the end of the month, used to get the final result of the month'
)
    COMMENT 'Number of active devices'
    ROW format delimited FIELDS TERMINATED BY '\t'
    location '/warehouse/gmall/ads/ads_uv_count/';



DROP TABLE
    IF EXISTS ads_new_mid_count;

CREATE EXTERNAL TABLE ads_new_mid_count
(
    `create_date`   STRING COMMENT '创建时间',
    `new_mid_count` BIGINT COMMENT '新增设备数量'
)
    COMMENT '每日新增设备信息数量'
    ROW fORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_new_mid_count/';


DROP TABLE
    IF EXISTS ads_new_mid_count;

CREATE EXTERNAL TABLE ads_new_mid_count
(
    `create_date`   string COMMENT 'create time',
    `new_mid_count` BIGINT COMMENT 'Number of new equipment'
)
    COMMENT 'The number of new device information added daily'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_new_mid_count/';


DROP TABLE
    IF EXISTS ads_silent_count;

CREATE external TABLE ads_silent_count
(
    `dt`           string COMMENT '统计日期',
    `silent_count` BIGINT COMMENT '沉默设备数'
)
    COMMENT '沉默用户数'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_silent_count';



DROP TABLE
    IF EXISTS ads_silent_count;

CREATE external TABLE ads_silent_count
(
    `dt`           string COMMENT 'Statistic Date',
    `silent_count` BIGINT COMMENT 'number of silent devices'
)
    COMMENT 'Number of silent users'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_silent_count';



DROP TABLE
    IF EXISTS ads_back_count;

CREATE external TABLE ads_back_count
(
    `dt`            string COMMENT '统计日期',
    `wk_dt`         string COMMENT '统计日期所在周',
    `wastage_count` BIGINT COMMENT '回流设备数'
)
    COMMENT '本周回流用户数'
    ROW format delimited FIELDS TERMINATED BY '\t'
    location '/warehouse/gmall/ads/ads_back_count';

DROP TABLE
    IF EXISTS ads_back_count;

CREATE external TABLE ads_back_count
(
    `dt`            string COMMENT 'Statistic Date',
    `wk_dt`         string COMMENT 'Week of the statistical date',
    `wastage_count` BIGINT COMMENT 'Number of return equipment'
)
    COMMENT 'Number of returning users this week'
    ROW format delimited FIELDS TERMINATED BY '\t'
    location '/warehouse/gmall/ads/ads_back_count';


DROP TABLE
    IF EXISTS ads_wastage_count;

CREATE external TABLE ads_wastage_count
(
    `dt`            string COMMENT '统计日期',
    `wastage_count` BIGINT COMMENT '流失设备数'
)
    COMMENT '流失用户数'
    ROW format delimited FIELDS TERMINATED BY '\t'
    location '/warehouse/gmall/ads/ads_wastage_count';


CREATE external TABLE ads_wastage_count
(
    `dt`            string COMMENT 'Statistic Date',
    `wastage_count` BIGINT COMMENT 'Number of lost devices'
)
    COMMENT 'Number of lost users'
    ROW format delimited FIELDS TERMINATED BY '\t'
    location '/warehouse/gmall/ads/ads_wastage_count';


DROP TABLE
    IF EXISTS ads_user_retention_day_rate;

CREATE external TABLE ads_user_retention_day_rate
(
    `stat_date`       string COMMENT '统计日期',
    `create_date`     string COMMENT '设备新增日期',
    `retention_day`   INT COMMENT '截止当前日期留存天数',
    `retention_count` BIGINT COMMENT '留存数量',
    `new_mid_count`   BIGINT COMMENT '设备新增数量',
    `retention_ratio` DECIMAL(10, 2) COMMENT '留存率'
)
    COMMENT '每日用户留存情况'
    ROW format delimited FIELDS TERMINATED BY '\t'
    location '/warehouse/gmall/ads/ads_user_retention_day_rate/';


CREATE external TABLE ads_user_retention_day_rate
(
    `stat_date`       string COMMENT 'Statistic date',
    `create_date`     string COMMENT 'Device new date',
    `retention_day`   INT COMMENT 'Retention days as of the current date',
    `retention_count` BIGINT COMMENT 'retention quantity',
    `new_mid_count`   BIGINT COMMENT 'The number of new equipment',
    `retention_ratio` DECIMAL(10, 2) COMMENT 'retention rate'
)
    COMMENT 'Daily user retention status'
    ROW format delimited FIELDS TERMINATED BY '\t'
    location '/warehouse/gmall/ads/ads_user_retention_day_rate/';

DROP TABLE
    IF EXISTS ads_continuity_wk_count;

CREATE external TABLE ads_continuity_wk_count
(
    `dt`               string COMMENT '统计日期,一般用结束周周日日期,如果每天计算一次,可用当天日 期',
    `wk_dt`            string COMMENT '持续时间',
    `continuity_count` BIGINT COMMENT '活跃次数'
)
    COMMENT '最近连续三周活跃用户数'
    ROW format delimited FIELDS TERMINATED BY '\t'
    location '/warehouse/gmall/ads/ads_continuity_wk_count';


CREATE external TABLE ads_continuity_wk_count
(
    `dt`               string COMMENT 'Statistic date, generally use the ending week and Sunday date, if it is calculated once a day, the current date can be used',
    `wk_dt`            string COMMENT 'Duration',
    `continuity_count` BIGINT COMMENT 'Active count'
)
    COMMENT 'Number of active users in the last three consecutive weeks'
    ROW format delimited FIELDS TERMINATED BY '\t'
    location '/warehouse/gmall/ads/ads_continuity_wk_count';



DROP TABLE
    IF EXISTS ads_continuity_uv_count;

CREATE external TABLE ads_continuity_uv_count
(
    `dt`               string COMMENT '统计日期',
    `wk_dt`            string COMMENT '最近 7 天日期',
    `continuity_count` BIGINT
)
    COMMENT '连续活跃设备数'
    ROW format delimited FIELDS TERMINATED BY '\t'
    location '/warehouse/gmall/ads/ads_continuity_uv_count';


CREATE external TABLE ads_continuity_uv_count
(
    `dt`               string COMMENT 'Statistic Date',
    `wk_dt`            string COMMENT 'Last 7 days date',
    `continuity_count` BIGINT
)
    COMMENT 'Number of continuous active devices'
    ROW format delimited FIELDS TERMINATED BY '\t'
    location '/warehouse/gmall/ads/ads_continuity_uv_count';