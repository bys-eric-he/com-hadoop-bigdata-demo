#!/bin/bash

APP=demo_database_gmall
hive=/usr/local/hive-2.3.7/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ]; then
  do_date=$2
else
  do_date=$(date -d "-1 day" +%F)
fi

sql1=" load data inpath '/eric/$APP/hive_data/order_info/$do_date' OVERWRITE into table ${APP}.ods_order_info partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/order_detail/$do_date' OVERWRITE into table ${APP}.ods_order_detail partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/sku_info/$do_date' OVERWRITE into table ${APP}.ods_sku_info partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/user_info/$do_date' OVERWRITE into table ${APP}.ods_user_info partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/payment_info/$do_date' OVERWRITE into table ${APP}.ods_payment_info partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/base_category_1/$do_date' OVERWRITE into table ${APP}.ods_base_category_1 partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/base_category_2/$do_date' OVERWRITE into table ${APP}.ods_base_category_2 partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/base_category_3/$do_date' OVERWRITE into table ${APP}.ods_base_category_3 partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/base_trademark/$do_date' OVERWRITE into table ${APP}.ods_base_trademark partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/activity_info/$do_date' OVERWRITE into table ${APP}.ods_activity_info partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/activity_order/$do_date' OVERWRITE into table ${APP}.ods_activity_order partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/cart_info/$do_date' OVERWRITE into table ${APP}.ods_cart_info partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/comment_info/$do_date' OVERWRITE into table ${APP}.ods_comment_info partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/coupon_info/$do_date' OVERWRITE into table ${APP}.ods_coupon_info partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/coupon_use/$do_date' OVERWRITE into table ${APP}.ods_coupon_use partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/favor_info/$do_date' OVERWRITE into table ${APP}.ods_favor_info partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/order_refund_info/$do_date' OVERWRITE into table ${APP}.ods_order_refund_info partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/order_status_log/$do_date' OVERWRITE into table ${APP}.ods_order_status_log partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/spu_info/$do_date' OVERWRITE into table ${APP}.ods_spu_info partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/activity_rule/$do_date' OVERWRITE into table ${APP}.ods_activity_rule partition(dt='$do_date');
load data inpath '/eric/$APP/hive_data/base_dic/$do_date' OVERWRITE into table ${APP}.ods_base_dic partition(dt='$do_date'); "

sql2=" load data inpath '/eric/$APP/hive_data/base_province/$do_date' OVERWRITE into table ${APP}.ods_base_province;
load data inpath '/eric/$APP/hive_data/base_region/$do_date' OVERWRITE into table ${APP}.ods_base_region;"

echo "===开始导入日志日期为 $do_date 的数据==="
case $1 in
"first")
  {

    $hive -e "$sql1"
    $hive -e "$sql2"
  }
  ;;

"all") {
  $hive -e "$sql1"
} ;;
esac
echo "===日志日期为 $do_date 的数据导入完成==="

