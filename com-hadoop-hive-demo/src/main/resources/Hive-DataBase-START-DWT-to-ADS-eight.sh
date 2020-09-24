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
INSERT INTO TABLE ${APP}.ads_continuity_uv_count SELECT
	'$do_date',
	concat(
		date_add('$do_date' ,- 6),
		'_',
		'$do_date'
	),
	count(*)
FROM
	(
		SELECT
			mid_id
		FROM
			(
				SELECT
					mid_id
				FROM
					(
						SELECT
							mid_id,
							date_sub(dt, rank) date_dif
						FROM
							(
								SELECT
									mid_id,
									dt,
									rank () over (PARTITION BY mid_id ORDER BY dt) rank
								FROM
									${APP}.dws_uv_detail_daycount
								WHERE
									dt >= date_add('$do_date' ,- 6)
								AND dt <= '$do_date'
							) t1
					) t2
				GROUP BY
					mid_id,
					date_dif
				HAVING
					count(*) >= 3
			) t3
		GROUP BY
			mid_id
	) t4;"

echo "===START TO TAKE The Number of continuous active devices DATA From $do_date ==="
$hive -e "$sql"
echo "===Finished TO TAKE The Number of continuous active devices DATA From $do_date ==="
