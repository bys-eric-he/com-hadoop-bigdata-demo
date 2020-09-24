#!/bin/bash
# 数据库名称
APP=demo_database_gmall

# hive目录
hive=/usr/local/hive-2.3.7/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ]; then
  do_date=$1
else
  do_date=$(date -d "0 day" +%F)
fi

#每日设备行为
sql="
INSERT overwrite TABLE ${APP}.dws_uv_detail_daycount PARTITION (dt = '$do_date') SELECT
	mid_id,
	concat_ws('|', collect_set(user_id)) user_id,
	concat_ws(
		'|',
		collect_set (version_code)
	) version_code,
	concat_ws(
		'|',
		collect_set (version_name)
	) version_name,
	concat_ws('|', collect_set(lang)) lang,
	concat_ws('|', collect_set(source)) source,
	concat_ws('|', collect_set(os)) os,
	concat_ws('|', collect_set(area)) area,
	concat_ws('|', collect_set(model)) model,
	concat_ws('|', collect_set(brand)) brand,
	concat_ws(
		'|',
		collect_set (sdk_version)
	) sdk_version,
	concat_ws('|', collect_set(gmail)) gmail,
	concat_ws(
		'|',
		collect_set (height_width)
	) height_width,
	concat_ws('|', collect_set(app_time)) app_time,
	concat_ws('|', collect_set(network)) network,
	concat_ws('|', collect_set(lng)) lng,
	concat_ws('|', collect_set(lat)) lat,
	count(*) login_count
FROM
	${APP}.dwd_start_log
WHERE
	dt = '$do_date'
GROUP BY
	mid_id;"

echo "===START to TAKE User Daily Device behavior DATA From $do_date ==="
$hive -e "$sql"
echo "===END to TAKE User Daily Device behavior DATA From $do_date ==="
