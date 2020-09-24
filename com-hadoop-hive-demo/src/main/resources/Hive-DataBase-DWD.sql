USE demo_database_gmall;

/*-------------------DWD层 明细数据层，对ODS层进行数据清洗（用户行为数据）-----------------*/

DROP TABLE
    IF EXISTS dwd_start_log;

CREATE EXTERNAL TABLE dwd_start_log
(
    `mid_id`       STRING,
    `user_id`      STRING,
    `version_code` STRING,
    `version_name` STRING,
    `lang`         STRING,
    `source`       STRING,
    `os`           STRING,
    `area`         STRING,
    `model`        STRING,
    `brand`        STRING,
    `sdk_version`  STRING,
    `gmail`        STRING,
    `height_width` STRING,
    `app_time`     STRING,
    `network`      STRING,
    `lng`          STRING,
    `lat`          STRING,
    `entry`        STRING,
    `open_ad_type` STRING,
    `action`       STRING,
    `loading_time` STRING,
    `detail`       STRING,
    `extend1`      STRING
)
    COMMENT '启动日志基础明细表'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET //说明：数据采用 parquet 存储方式，是可以支持切片的，不需要再对数据创建索引。
    LOCATION '/warehouse/gmall/dwd/dwd_start_log/';


DROP TABLE
    IF EXISTS dwd_base_event_log;

CREATE EXTERNAL TABLE dwd_base_event_log
(
    `mid_id`       STRING,
    `user_id`      STRING,
    `version_code` STRING,
    `version_name` STRING,
    `lang`         STRING,
    `source`       STRING,
    `os`           STRING,
    `area`         STRING,
    `model`        STRING,
    `brand`        STRING,
    `sdk_version`  STRING,
    `gmail`        STRING,
    `height_width` STRING,
    `app_time`     STRING,
    `network`      STRING,
    `lng`          STRING,
    `lat`          STRING,
    `event_name`   STRING,
    `event_json`   STRING,
    `server_time`  STRING
)
    COMMENT '事件日志基础明细表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_base_event_log/';


DROP TABLE
    IF EXISTS dwd_comment_log;

CREATE EXTERNAL TABLE dwd_comment_log
(
    `mid_id`       STRING,
    `user_id`      STRING,
    `version_code` STRING,
    `version_name` STRING,
    `lang`         STRING,
    `source`       STRING,
    `os`           STRING,
    `area`         STRING,
    `model`        STRING,
    `brand`        STRING,
    `sdk_version`  STRING,
    `gmail`        STRING,
    `height_width` STRING,
    `app_time`     STRING,
    `network`      STRING,
    `lng`          STRING,
    `lat`          STRING,
    `comment_id`   INT,
    `userid`       INT,
    `p_comment_id` INT,
    `content`      STRING,
    `addtime`      STRING,
    `other_id`     INT,
    `praise_count` INT,
    `reply_count`  INT,
    `server_time`  STRING
)
    COMMENT '评论表'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_comment_log/';



DROP TABLE
    IF EXISTS dwd_praise_log;

CREATE EXTERNAL TABLE dwd_praise_log
(
    `mid_id`       STRING,
    `user_id`      STRING,
    `version_code` STRING,
    `version_name` STRING,
    `lang`         STRING,
    `source`       STRING,
    `os`           STRING,
    `area`         STRING,
    `model`        STRING,
    `brand`        STRING,
    `sdk_version`  STRING,
    `gmail`        STRING,
    `height_width` STRING,
    `app_time`     STRING,
    `network`      STRING,
    `lng`          STRING,
    `lat`          STRING,
    `id`           STRING,
    `userid`       STRING,
    `target_id`    STRING,
    `type`         STRING,
    `add_time`     STRING,
    `server_time`  STRING
)
    COMMENT '点赞表'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET LOCATION '/warehouse/gmall/dwd/dwd_praise_log/';

DROP TABLE
    IF EXISTS dwd_active_background_log;

CREATE EXTERNAL TABLE dwd_active_background_log
(
    `mid_id`        STRING,
    `user_id`       STRING,
    `version_code`  STRING,
    `version_name`  STRING,
    `lang`          STRING,
    `source`        STRING,
    `os`            STRING,
    `area`          STRING,
    `model`         STRING,
    `brand`         STRING,
    `sdk_version`   STRING,
    `gmail`         STRING,
    `height_width`  STRING,
    `app_time`      STRING,
    `network`       STRING,
    `lng`           STRING,
    `lat`           STRING,
    `active_source` STRING,
    `server_time`   STRING
)
    COMMENT '用户后台活跃表'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET LOCATION '/warehouse/gmall/dwd/dwd_background_log/';