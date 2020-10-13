


# --update-mode allowinsert --update-key dt 两个参数同时使用，表示若以–update-key指定的列匹配到存在，则更新；若匹配不到不存在，则插入（此时mysql必须指定主键，否则会产生数据全部重复导入的情况）
# --fields-terminated-by '\t' 表示Hive 中被导出的文件字段的分隔符

# ads_back_count 表
sqoop export --connect jdbc:mysql://172.12.0.3:3306/demo_database_gmall --username root --password root --table ads_back_count --export-dir /warehouse/gmall/ads/ads_back_count --update-mode allowinsert --update-key dt --fields-terminated-by '\t'

# ads_uv_count 表
sqoop export --connect jdbc:mysql://172.12.0.3:3306/demo_database_gmall --username root --password root --table ads_uv_count --export-dir /warehouse/gmall/ads/ads_uv_count --update-mode allowinsert --update-key dt --fields-terminated-by '\t'

# ads_continuity_uv_count 表
sqoop export --connect jdbc:mysql://172.12.0.3:3306/demo_database_gmall --username root --password root --table ads_continuity_uv_count --export-dir /warehouse/gmall/ads/ads_continuity_uv_count --update-mode allowinsert --update-key dt --fields-terminated-by '\t'

# ads_continuity_wk_count 表
sqoop export --connect jdbc:mysql://172.12.0.3:3306/demo_database_gmall --username root --password root --table ads_continuity_wk_count --export-dir /warehouse/gmall/ads/ads_continuity_wk_count --update-mode allowinsert --update-key dt --fields-terminated-by '\t'

# ads_new_mid_count 表
sqoop export --connect jdbc:mysql://172.12.0.3:3306/demo_database_gmall --username root --password root --table ads_new_mid_count --export-dir /warehouse/gmall/ads/ads_new_mid_count --update-mode allowinsert --update-key create_date --fields-terminated-by '\t'

# ads_silent_count 表
sqoop export --connect jdbc:mysql://172.12.0.3:3306/demo_database_gmall --username root --password root --table ads_silent_count --export-dir /warehouse/gmall/ads/ads_silent_count --update-mode allowinsert --update-key dt --fields-terminated-by '\t'

# ads_user_retention_day_rate 表

# --input-null-string '\\N' --input-null-non-string '\\N' 用sqoop从hive读取数据到MySQL是无法导入null值的解决办法
# 因为hive中为null的是以\N代替的，所以你在导入到MySql时，需要加上两个参数：--input-null-string '\\N' --input-null-non-string '\\N'，多加一个'\'，是为转义。
sqoop export --connect jdbc:mysql://172.12.0.3:3306/demo_database_gmall --username root --password root --table ads_user_retention_day_rate --export-dir /warehouse/gmall/ads/ads_user_retention_day_rate --update-mode allowinsert --update-key stat_date,create_date --fields-terminated-by '\t' --input-null-string '\\N' --input-null-non-string '\\N'

# ads_wastage_count 表
sqoop export --connect jdbc:mysql://172.12.0.3:3306/demo_database_gmall --username root --password root --table ads_wastage_count --export-dir /warehouse/gmall/ads/ads_wastage_count --update-mode allowinsert --update-key dt --fields-terminated-by '\t'

