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

#设备主题宽表
sql="
INSERT overwrite TABLE ${APP}.dwt_uv_topic SELECT
	nvl (new.mid_id, old.mid_id),
	nvl (new.user_id, old.user_id),
	nvl (
		new.version_code,
		old.version_code
	),
	nvl (
		new.version_name,
		old.version_name
	),
	nvl (new.lang, old.lang),
	nvl (new.source, old.source),
	nvl (new.os, old.os),
	nvl (new.area, old.area),
	nvl (new.model, old.model),
	nvl (new.brand, old.brand),
	nvl (
		new.sdk_version,
		old.sdk_version
	),
	nvl (new.gmail, old.gmail),
	nvl (
		new.height_width,
		old.height_width
	),
	nvl (new.app_time, old.app_time),
	nvl (new.network, old.network),
	nvl (new.lng, old.lng),
	nvl (new.lat, old.lat),

IF (
	old.mid_id IS NULL,
	$do_date,
	old.login_date_first
),

IF (
	new.mid_id IS NOT NULL,
	$do_date,
	old.login_date_last
),

IF (
	new.mid_id IS NOT NULL,
	new.login_count,
	0
),
 nvl (old.login_count, 0) +
IF (new.login_count > 0, 1, 0)
FROM
	(SELECT * FROM ${APP}.dwt_uv_topic) old
FULL OUTER JOIN (
	SELECT
		*
	FROM
		${APP}.dws_uv_detail_daycount
	WHERE
		dt = $do_date
) new ON old.mid_id = new.mid_id;"

echo "===START to TAKE User Device subject wide table DATA From $do_date ==="
$hive -e "$sql"
echo "===END to TAKE User Device subject wide table DATA From $do_date ==="
