-- 在MySQL创建与Hive同名的数据库
CREATE DATABASE `demo_database_gmall` DEFAULT CHARACTER
SET utf8 COLLATE utf8_general_ci;

USE `demo_database_gmall`;

CREATE TABLE ads_uv_count (
	`dt` VARCHAR (20) COMMENT '统计日期',
	`day_count` BIGINT COMMENT '当日用户数量',
	`wk_count` BIGINT COMMENT '当周用户数量',
	`mn_count` BIGINT COMMENT '当月用户数量',
	`is_weekend` VARCHAR (2) COMMENT 'Y,N 是否是周末,用于得到本周最终结果',
	`is_monthend` VARCHAR (2) COMMENT 'Y,N 是否是月末,用于得到本月最终结果'
) COMMENT = '活跃设备数统计表';

CREATE TABLE ads_new_mid_count (
	`create_date` VARCHAR (20) COMMENT '创建时间',
	`new_mid_count` BIGINT COMMENT '新增设备数量'
) COMMENT '每日新增设备信息数量统计表';

CREATE TABLE ads_silent_count (
	`dt` VARCHAR (20) COMMENT '统计日期',
	`silent_count` BIGINT COMMENT '沉默设备数'
) COMMENT '沉默用户数统计表';

CREATE TABLE ads_back_count (
	`dt` VARCHAR (20) COMMENT '统计日期',
	`wastage_count` BIGINT COMMENT '回流设备数'
) COMMENT '本周回流用户数统计表';

CREATE TABLE ads_wastage_count (
	`dt` VARCHAR (20) COMMENT '统计日期',
	`wastage_count` BIGINT COMMENT '流失设备数'
) COMMENT '流失用户数统计表';

CREATE TABLE ads_user_retention_day_rate (
	`stat_date` VARCHAR (20) COMMENT '统计日期',
	`create_date` VARCHAR (20) COMMENT '设备新增日期',
	`retention_day` INT COMMENT '截止当前日期留存天数',
	`retention_count` BIGINT COMMENT '留存数量',
	`new_mid_count` BIGINT COMMENT '设备新增数量',
	`retention_ratio` DECIMAL (10, 2) COMMENT '留存率'
) COMMENT '每日用户留存情况统计表';

CREATE TABLE ads_continuity_wk_count (
	`dt` VARCHAR (20) COMMENT '统计日期,一般用结束周周日日期,如果每天计算一次,可用当天日期',
	`wk_dt` VARCHAR (100) COMMENT '持续时间',
	`continuity_count` BIGINT COMMENT '活跃次数'
) COMMENT '最近连续三周活跃用户数统计';

CREATE TABLE ads_continuity_uv_count (
	`dt` VARCHAR (20) COMMENT '统计日期',
	`wk_dt` VARCHAR (100) COMMENT '最近 7 天日期',
	`continuity_count` BIGINT
) COMMENT '连续活跃设备数';