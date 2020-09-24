/*------------------创建数据库--------------------*/
CREATE DATABASE demo_database_gmall;

USE demo_database_gmall;

/*-------------------ODS 原始数据层，存放原始数据（用户行为数据）-----------------*/

ALTER TABLE ods_start_log
    SET TBLPROPERTIES ('EXTERNAL' = 'FALSE');

DROP TABLE
    IF EXISTS ods_start_log;

CREATE EXTERNAL TABLE ods_start_log
(
    `line` STRING
)
    COMMENT '启动日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_start_log';


ALTER TABLE ods_event_log
    SET TBLPROPERTIES ('EXTERNAL' = 'FALSE');

DROP TABLE
    IF EXISTS ods_event_log;

CREATE EXTERNAL TABLE ods_event_log
(
    `line` STRING
)
    COMMENT '事件日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_event_log';


/*---------------------ODS 层（业务数据）--------------------*/

DROP TABLE
    IF EXISTS ods_order_info;

CREATE EXTERNAL TABLE ods_order_info
(
    `id`                    STRING COMMENT '订单号',
    `final_total_amount`    DECIMAL(10, 2) COMMENT '订单金额',
    `order_status`          STRING COMMENT '订单状态',
    `user_id`               STRING COMMENT '用户 id',
    `out_trade_no`          STRING COMMENT '支付流水号',
    `create_time`           STRING COMMENT '创建时间',
    `operate_time`          STRING COMMENT '操作时间',
    `province_id`           STRING COMMENT '省份 ID',
    `benefit_reduce_amount` DECIMAL(10, 2) COMMENT '优惠金额',
    `original_total_amount` DECIMAL(10, 2) COMMENT '原价金额',
    `feight_fee`            DECIMAL(10, 2) COMMENT '运费'
)
    COMMENT '订单表'
    PARTITIONED BY (`dt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_order_info/';


DROP TABLE
    IF EXISTS ods_order_detail;

CREATE EXTERNAL TABLE ods_order_detail
(
    `id`          STRING COMMENT '订单编号',
    `order_id`    STRING COMMENT '订单号',
    `user_id`     STRING COMMENT '用户 id',
    `sku_id`      STRING COMMENT '商品 id',
    `sku_name`    STRING COMMENT '商品名称',
    `order_price` DECIMAL(10, 2) COMMENT '商品价格',
    `sku_num`     BIGINT COMMENT '商品数量',
    `create_time` STRING COMMENT '创建时间'
)
    COMMENT '订单详情表'
    PARTITIONED BY (`dt` string)
    ROW format delimited FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_order_detail/';



DROP TABLE
    IF EXISTS ods_sku_info;

CREATE EXTERNAL TABLE ods_sku_info
(
    `id`           STRING COMMENT 'skuId',
    `spu_id`       STRING COMMENT 'spuid',
    `price`        DECIMAL(10, 2) COMMENT '价格',
    `sku_name`     STRING COMMENT '商品名称',
    `sku_desc`     STRING COMMENT '商品描述',
    `weight`       STRING COMMENT '重量',
    `tm_id`        STRING COMMENT '品牌 id',
    `category3_id` STRING COMMENT '品类 id',
    `create_time`  STRING COMMENT '创建时间'
)
    COMMENT 'SKU 商品表'
    PARTITIONED BY (`dt` string)
    ROW format delimited FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_sku_info/';


DROP TABLE
    IF EXISTS ods_user_info;

CREATE EXTERNAL TABLE ods_user_info
(
    `id`           STRING COMMENT '用户 id',
    `name`         STRING COMMENT '姓名',
    `birthday`     STRING COMMENT '生日',
    `gender`       STRING COMMENT '性别',
    `email`        STRING COMMENT '邮箱',
    `user_level`   STRING COMMENT '用户等级',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间'
)
    COMMENT '用户表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_user_info/';



DROP TABLE
    IF EXISTS ods_base_category_1;

CREATE EXTERNAL TABLE ods_base_category_1
(
    `id`   STRING COMMENT 'id',
    `name` STRING COMMENT '名称'
)
    COMMENT '商品一级分类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_base_category_1/';


DROP TABLE
    IF EXISTS ods_base_category_2;

CREATE EXTERNAL TABLE ods_base_category_2
(
    `id`          STRING COMMENT ' id',
    `name`        STRING COMMENT '名称',
    category_1_id STRING COMMENT '一级品类 id'
)
    COMMENT '商品二级分类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    location '/warehouse/gmall/ods/ods_base_category_2/';


DROP TABLE
    IF EXISTS ods_base_category_3;

CREATE EXTERNAL TABLE ods_base_category_3
(
    `id`          STRING COMMENT ' id',
    `name`        STRING COMMENT '名称',
    category_2_id STRING COMMENT '二级品类 id'
)
    COMMENT '商品三级分类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_base_category_3/';



DROP TABLE
    IF EXISTS ods_payment_info;

CREATE EXTERNAL TABLE ods_payment_info
(
    `id`              BIGINT COMMENT '编号',
    `out_trade_no`    STRING COMMENT '对外业务编号',
    `order_id`        STRING COMMENT '订单编号',
    `user_id`         STRING COMMENT '用户编号',
    `alipay_trade_no` STRING COMMENT '支付宝交易流水编号',
    `total_amount`    DECIMAL(16, 2) COMMENT '支付金额',
    `subject`         STRING COMMENT '交易内容',
    `payment_type`    STRING COMMENT '支付类型',
    `payment_time`    STRING COMMENT '支付时间'
)
    COMMENT '支付流水表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_payment_info/';



DROP TABLE
    IF EXISTS ods_base_province;

CREATE EXTERNAL TABLE ods_base_province
(
    `id`        BIGINT COMMENT '编号',
    `name`      STRING COMMENT '省份名称',
    `region_id` STRING COMMENT '地区 ID',
    `area_code` STRING COMMENT '地区编码',
    `iso_code`  STRING COMMENT 'iso 编码'
)
    COMMENT '省份表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_base_province/';



DROP TABLE
    IF EXISTS ods_base_region;

CREATE EXTERNAL TABLE ods_base_region
(
    `id`          BIGINT COMMENT '编号',
    `region_name` STRING COMMENT '地区名称'
)
    COMMENT '地区表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_base_region/';


DROP TABLE
    IF EXISTS ods_base_trademark;

CREATE EXTERNAL TABLE ods_base_trademark
(
    `tm_id`   BIGINT COMMENT '编号',
    `tm_name` STRING COMMENT '品牌名称'
)
    COMMENT '品牌表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_base_trademark/';



DROP TABLE
    IF EXISTS ods_order_status_log;

CREATE EXTERNAL TABLE ods_order_status_log
(
    `id`           BIGINT COMMENT '编号',
    `order_id`     STRING COMMENT '订单 ID',
    `order_status` STRING COMMENT '订单状态',
    `operate_time` STRING COMMENT '修改时间'
)
    COMMENT '订单状态表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_order_status_log/';



DROP TABLE
    IF EXISTS ods_spu_info;

CREATE EXTERNAL TABLE ods_spu_info
(
    `id`           STRING COMMENT 'spuid',
    `spu_name`     STRING COMMENT 'spu 名称',
    `category3_id` STRING COMMENT '品类 id',
    `tm_id`        STRING COMMENT '品牌 id'
) COMMENT 'SPU 商品表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_spu_info/';


DROP TABLE
    IF EXISTS ods_comment_info;

CREATE external TABLE ods_comment_info
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户 ID',
    `sku_id`      STRING COMMENT '商品 sku',
    `spu_id`      STRING COMMENT '商品 spu',
    `order_id`    STRING COMMENT '订单 ID',
    `appraise`    STRING COMMENT '评价',
    `create_time` STRING COMMENT '评价时间'
)
    COMMENT '商品评论表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_comment_info/';



DROP TABLE
    IF EXISTS ods_order_refund_info;

CREATE EXTERNAL TABLE ods_order_refund_info
(
    `id`                 STRING COMMENT '编号',
    `user_id`            STRING COMMENT '用户 ID',
    `order_id`           STRING COMMENT '订单 ID',
    `sku_id`             STRING COMMENT '商品 ID',
    `refund_type`        STRING COMMENT '退款类型',
    `refund_num`         BIGINT COMMENT '退款件数',
    `refund_amount`      DECIMAL(16, 2) COMMENT '退款金额',
    `refund_reason_type` STRING COMMENT '退款原因类型',
    `create_time`        STRING COMMENT '退款时间'
)
    COMMENT '退单表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_order_refund_info/';



DROP TABLE
    IF EXISTS ods_cart_info;

CREATE EXTERNAL TABLE ods_cart_info
(
    `id`           STRING COMMENT '编号',
    `user_id`      STRING COMMENT '用户 id',
    `sku_id`       STRING COMMENT 'skuid',
    `cart_price`   STRING COMMENT '放入购物车时价格',
    `sku_num`      STRING COMMENT '数量',
    `sku_name`     STRING COMMENT 'sku 名称 (冗余)',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间',
    `is_ordered`   STRING COMMENT '是否已经下单',
    `order_time`   STRING COMMENT '下单时间'
)
    COMMENT '加购表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_cart_info/';


DROP TABLE
    IF EXISTS ods_favor_info;

CREATE EXTERNAL TABLE ods_favor_info
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户 id',
    `sku_id`      STRING COMMENT 'skuid',
    `spu_id`      STRING COMMENT 'spuid',
    `is_cancel`   STRING COMMENT '是否取消',
    `create_time` STRING COMMENT '收藏时间',
    `cancel_time` STRING COMMENT '取消时间'
)
    COMMENT '商品收藏表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_favor_info/';


DROP TABLE
    IF EXISTS ods_coupon_use;

CREATE EXTERNAL TABLE ods_coupon_use
(
    `id`            STRING COMMENT '编号',
    `coupon_id`     STRING COMMENT '优惠券 ID',
    `user_id`       STRING COMMENT 'skuid',
    `order_id`      STRING COMMENT 'spuid',
    `coupon_status` STRING COMMENT '优惠券状态',
    `get_time`      STRING COMMENT '领取时间',
    `using_time`    STRING COMMENT '使用时间(下单)',
    `used_time`     STRING COMMENT '使用时间(支付)'
)
    COMMENT '优惠券领用表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_coupon_use/';


DROP TABLE
    IF EXISTS ods_coupon_info;

CREATE EXTERNAL TABLE ods_coupon_info
(
    `id`               STRING COMMENT '购物券编号',
    `coupon_name`      STRING COMMENT '购物券名称',
    `coupon_type`      STRING COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` STRING COMMENT '满额数',
    `condition_num`    STRING COMMENT '满件数',
    `activity_id`      STRING COMMENT '活动编号',
    `benefit_amount`   STRING COMMENT '减金额',
    `benefit_discount` STRING COMMENT '折扣',
    `create_time`      STRING COMMENT '创建时间',
    `range_type`       STRING COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `spu_id`           STRING COMMENT '商品 id',
    `tm_id`            STRING COMMENT '品牌 id',
    `category3_id`     STRING COMMENT '品类 id',
    `limit_num`        STRING COMMENT '最多领用次数',
    `operate_time`     STRING COMMENT '修改时间',
    `expire_time`      STRING COMMENT '过期时间'
)
    COMMENT '优惠券表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_coupon_info/';


DROP TABLE
    IF EXISTS ods_activity_info;

CREATE EXTERNAL TABLE ods_activity_info
(
    `id`            STRING COMMENT '编号',
    `activity_name` STRING COMMENT '活动名称',
    `activity_type` STRING COMMENT '活动类型',
    `start_time`    STRING COMMENT '开始时间',
    `end_time`      STRING COMMENT '结束时间',
    `create_time`   STRING COMMENT '创建时间'
)
    COMMENT '活动表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_activity_info/';


DROP TABLE
    IF EXISTS ods_activity_order;

CREATE EXTERNAL TABLE ods_activity_order
(
    `id`          STRING COMMENT '编号',
    `activity_id` STRING COMMENT '优惠券 ID',
    `order_id`    STRING COMMENT 'skuid',
    `create_time` STRING COMMENT '领取时间'
)
    COMMENT '活动订单关联表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_activity_order/';



DROP TABLE
    IF EXISTS ods_activity_rule;

CREATE EXTERNAL TABLE ods_activity_rule
(
    `id`               STRING COMMENT '编号',
    `activity_id`      STRING COMMENT '活动 ID',
    `condition_amount` STRING COMMENT '满减金额',
    `condition_num`    STRING COMMENT '满减件数',
    `benefit_amount`   STRING COMMENT '优惠金额',
    `benefit_discount` STRING COMMENT '优惠折扣',
    `benefit_level`    STRING COMMENT '优惠级别'
)
    COMMENT '优惠规则表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_activity_rule/';


DROP TABLE
    IF EXISTS ods_base_dic;

CREATE EXTERNAL TABLE ods_base_dic
(
    `dic_code`     STRING COMMENT '编号',
    `dic_name`     STRING COMMENT '编码名称',
    `parent_code`  STRING COMMENT '父编码',
    `create_time`  STRING COMMENT '创建日期',
    `operate_time` STRING COMMENT '操作日期'
)
    COMMENT '编码字典表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_base_dic/';