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
INSERT INTO TABLE ${APP}.ads_back_count SELECT
	'$do_date',
	count(*)
FROM
	(
		SELECT
			mid_id
		FROM
			${APP}.dwt_uv_topic
		WHERE
			login_date_last >= date_add(
				next_day ('$do_date', 'MO') ,- 7
			)
		AND login_date_last <= date_add(
			next_day ('$do_date', 'MO') ,- 1
		)
		AND login_date_first < date_add(
			next_day ('$do_date', 'MO') ,- 7
		)
	) current_wk
LEFT JOIN (
	SELECT
		mid_id
	FROM
		${APP}.dws_uv_detail_daycount
	WHERE
		dt >= date_add(
			next_day ('$do_date', 'MO') ,- 7 * 2
		)
	AND dt <= date_add(
		next_day ('$do_date', 'MO') ,- 7 - 1
	)
	GROUP BY
		mid_id
) last_wk ON current_wk.mid_id = last_wk.mid_id
WHERE
	last_wk.mid_id IS NULL;
"

echo "===START TO TAKE The Number of returning users this week DATA From $do_date ==="
$hive -e "$sql"
echo "===Finished TO TAKE The Number of returning users this week DATA From $do_date ==="
