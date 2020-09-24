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

INSERT INTO TABLE ${APP}.ads_uv_count SELECT
	'$do_date' dt,
	daycount.ct,
	wkcount.ct,
	mncount.ct,

IF (
	date_add(
		next_day ('$do_date', 'MO') ,- 1
	) = '$do_date',
	'Y',
	'N'
),

IF (
	last_day('$do_date') = '$do_date',
	'Y',
	'N'
)
FROM
	(
		SELECT
			'$do_date' dt,
			count(*) ct
		FROM
			${APP}.dwt_uv_topic
		WHERE
			login_date_last = '$do_date'
	) daycount
JOIN (
	SELECT
		'$do_date' dt,
		count(*) ct
	FROM
		${APP}.dwt_uv_topic
	WHERE
		login_date_last >= date_add(
			next_day ('$do_date', 'MO') ,- 7
		)
	AND login_date_last <= date_add(
		next_day ('$do_date', 'MO') ,- 1
	)
) wkcount ON daycount.dt = wkcount.dt
JOIN (
	SELECT
		'$do_date' dt,
		count(*) ct
	FROM
		${APP}.dwt_uv_topic
	WHERE
		date_format(login_date_last, 'yyyy-MM') = date_format('$do_date', 'yyyy-MM')
) mncount ON daycount.dt = mncount.dt; "

echo "===START TO TAKE Number of active devices DATA From $do_date ==="
$hive -e "$sql"
echo "===Finished TO TAKE Number of active devices DATA From $do_date ==="
