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

INSERT INTO TABLE ${APP}.ads_silent_count SELECT
	'$do_date',
	count(*)
FROM
	dwt_uv_topic
WHERE
	login_date_first = login_date_last
AND login_date_last <= date_add('$do_date' ,- 7);"

echo "===START TO TAKE The Number of silent users DATA From $do_date ==="
$hive -e "$sql"
echo "===Finished TO TAKE The Number of silent users DATA From $do_date ==="
