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
INSERT INTO TABLE ${APP}.ads_continuity_wk_count SELECT
	'$do_date',
	concat(
		date_add(
			next_day ('$do_date', 'MO') ,- 7 * 3
		),
		'_',
		date_add(
			next_day ('$do_date', 'MO') ,- 1
		)
	),
	count(*)
FROM
	(
		SELECT
			mid_id
		FROM
			(
				SELECT mid_id
				FROM
					${APP}.dws_uv_detail_daycount
				WHERE
					dt >= date_add(
						next_day ('$do_date', 'monday') ,- 7
					)
				AND dt <= date_add(
					next_day ('$do_date', 'monday') ,- 1
				)
				GROUP BY
					mid_id
				UNION ALL
					SELECT
						mid_id
					FROM
						${APP}.dws_uv_detail_daycount
					WHERE
						dt >= date_add(
							next_day ('$do_date', 'monday') ,- 7 * 2
						)
					AND dt <= date_add(
						next_day ('$do_date', 'monday') ,- 7 - 1
					)
					GROUP BY
						mid_id
					UNION ALL
						SELECT
							mid_id
						FROM
							${APP}.dws_uv_detail_daycount
						WHERE
							dt >= date_add(
								next_day ('$do_date', 'monday') ,- 7 * 3
							)
						AND dt <= date_add(
							next_day ('$do_date', 'monday') ,- 7 * 2 - 1
						)
						GROUP BY
							mid_id
			) t1
		GROUP BY
			mid_id
		HAVING
			count(*) = 3
	) t2;"

echo "===START TO TAKE The Number of active users in the last three consecutive weeks DATA From $do_date ==="
$hive -e "$sql"
echo "===Finished TO TAKE The Number of active users in the last three consecutive weeks DATA From $do_date ==="
