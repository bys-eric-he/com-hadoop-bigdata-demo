#!/bin/bash
# 数据库名称
APP=demo_database_gmall

# hive目录
hive=/usr/local/hive-2.3.7/bin/hive

# [ -n 变量值 ] 判断变量的值，是否为空
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ]; then
  do_date=$1
else
  do_date=$(date -d "-1 day" +%F)
fi


sql="
load data inpath '/eric/hive_data/$do_date/start_log.txt' overwrite into table ${APP}.ods_start_log partition(dt='$do_date');
load data inpath '/eric/hive_data/$do_date/event_log.txt' overwrite into table ${APP}.ods_event_log partition(dt='$do_date'); "

echo "===开始导入日志日期为 $do_date 的数据==="

$hive -e "$sql"

echo "===日志日期为 $do_date 的数据导入完成==="

