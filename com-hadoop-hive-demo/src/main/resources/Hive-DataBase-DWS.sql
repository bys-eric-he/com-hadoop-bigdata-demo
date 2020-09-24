
/*-----------DWS层 服务数据层，以DWD为基础按天进行轻度汇总（用户行为）--------------------*/

DROP TABLE
    IF EXISTS dws_uv_detail_daycount;

CREATE EXTERNAL TABLE dws_uv_detail_daycount
(
    `mid_id`       string COMMENT '设备唯一标识',
    `user_id`      string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang`         string COMMENT '系统语言',
    `source`       string COMMENT '渠道号',
    `os`           string COMMENT '安卓系统版本',
    `area`         string COMMENT '区域',
    `model`        string COMMENT '手机型号',
    `brand`        string COMMENT '手机品牌',
    `sdk_version`  string COMMENT 'sdkVersion',
    `gmail`        string COMMENT 'gmail',
    `height_width` string COMMENT '屏幕宽高',
    `app_time`     string COMMENT '客户端日志产生时的时间',
    `network`      string COMMENT '网络模式',
    `lng`          string COMMENT '经度',
    `lat`          string COMMENT '纬度',
    `login_count`  BIGINT COMMENT '活跃次数'
)
    COMMENT "每日设备行为"
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dws/dws_uv_detail_daycount';


DROP TABLE
    IF EXISTS dws_uv_detail_daycount;

CREATE EXTERNAL TABLE dws_uv_detail_daycount
(
    `mid_id` string COMMENT'Unique Device ID',
    `user_id` string COMMENT'User ID',
    `version_code` string COMMENT'Program version number',
    `version_name` string COMMENT'Program version name',
    `lang` string COMMENT'System language',
    `source` string COMMENT'channel number',
    `os` string COMMENT'Android system version',
    `area` string COMMENT'area',
    `model` string COMMENT'Mobile phone model',
    `brand` string COMMENT'Mobile phone brand',
    `sdk_version` string COMMENT'sdkVersion',
    `gmail` string COMMENT'gmail',
    `height_width` string COMMENT'Screen width and height',
    `app_time` string COMMENT'The time when the client log was generated',
    `network` string COMMENT'Network Mode',
    `lng` string COMMENT'Longitude',
    `lat` string COMMENT'Latitude',
    `login_count` BIGINT COMMENT'Active count'
)
    COMMENT "Daily Device Behavior"
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
    LOCATION'/warehouse/gmall/dws/dws_uv_detail_daycount';