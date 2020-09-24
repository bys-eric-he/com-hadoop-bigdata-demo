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

INSERT INTO TABLE ${APP}.ads_new_mid_count SELECT
	login_date_first,
	count(*)
FROM
	${APP}.dwt_uv_topic
WHERE
	login_date_first = '$do_date'
GROUP BY
	login_date_first;"

echo "===START TO TAKE The number of new device information added daily DATA From $do_date ==="
$hive -e "$sql"
echo "===Finished TO TAKE The number of new device information added daily DATA From $do_date ==="

