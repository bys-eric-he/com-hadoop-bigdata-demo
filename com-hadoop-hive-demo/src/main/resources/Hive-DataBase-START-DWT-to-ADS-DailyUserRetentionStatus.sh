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
INSERT INTO TABLE ${APP}.ads_user_retention_day_rate SELECT
	'$do_date' ,-- 统计日期 date_add('$do_date' ,- 1) ,-- 新增日期 1 ,-- 留存天数 sum(

		IF (
			login_date_first = date_add('$do_date' ,- 1)
			AND login_date_last = '$do_date',
			1,
			0
		)
	) ,-- '$do_date' 的 1 日留存数 sum(

		IF (
			login_date_first = date_add('$do_date' ,- 1),
			1,
			0
		)
	) ,-- '$do_date' 新增 sum(

		IF (
			login_date_first = date_add('$do_date' ,- 1)
			AND login_date_last = '$do_date',
			1,
			0
		)
	) / sum(

		IF (
			login_date_first = date_add('$do_date' ,- 1),
			1,
			0
		)
	) * 100
FROM
	${APP}.dwt_uv_topic UNION ALL SELECT
		'$do_date' ,-- 统计日期 date_add('$do_date' ,- 2) ,-- 新增日期 2 ,-- 留存天数
		sum(
			IF (
				login_date_first = date_add('$do_date' ,- 2)
				AND login_date_last = '$do_date',
				1,
				0
			)
		) ,-- '$do_date' 的 2 日留存数
		sum(
			IF (
				login_date_first = date_add('$do_date' ,- 2),
				1,
				0
			)
		) ,-- '$do_date' 新增
		sum(
			IF (
				login_date_first = date_add('$do_date' ,- 2)
				AND login_date_last = '$do_date',
				1,
				0
			)
		) / sum(
			IF (
				login_date_first = date_add('$do_date' ,- 2),
				1,
				0
			)
		) * 100
	FROM
		${APP}.dwt_uv_topic
	UNION ALL
		SELECT
			'$do_date' ,-- 统计日期 date_add('$do_date' ,- 3) ,-- 新增日期 3 ,-- 留存天数
			sum(
				IF (
					login_date_first = date_add('$do_date',- 3)
					AND login_date_last = '$do_date',
					1,
					0
				)
			) ,-- '$do_date' 的 3 日留存数
			sum(
				IF (
					login_date_first = date_add('$do_date',- 3),
					1,
					0
				)
			) ,-- '$do_date' 新增
			sum(
				IF (
					login_date_first = date_add('$do_date' ,- 3)
					AND login_date_last = '$do_date',
					1,
					0
				)
			) / sum(

				IF (
					login_date_first = date_add('$do_date' ,- 3),
					1,
					0
				)
			) * 100
		FROM
			${APP}.dwt_uv_topic;"

echo "===START TO TAKE The Daily user retention status DATA From $do_date ==="
$hive -e "$sql"
echo "===Finished TO TAKE The Daily user retention status DATA From $do_date ==="
