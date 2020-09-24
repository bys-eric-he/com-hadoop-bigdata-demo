#!/bin/bash
# 数据库名称
APP=demo_database_gmall

# hive目录
hive=/usr/local/hive-2.3.7/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ]; then
  do_date=$1
else
  do_date=$(date -d "-1 day" +%F)
fi

sql="
INSERT overwrite TABLE ${APP}.dwd_active_background_log PARTITION (dt = '$do_date') SELECT
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.kv.active_source') active_source,
	server_time
FROM
	${APP}.dwd_base_event_log
WHERE
	dt = '$do_date'
AND event_name = 'active_background';"

echo "===开始从事件明细中提取日期为 $do_date 的活跃用户数据==="
$hive -e "$sql"
echo "===从事件明细中提取日期为 $do_date 的活跃用户数据完成==="
