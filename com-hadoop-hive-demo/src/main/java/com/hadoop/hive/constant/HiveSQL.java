package com.hadoop.hive.constant;

import com.hadoop.hive.common.DateUtil;

/**
 * ETL数据抽取、清洗、统计脚本
 * Demo演示场景：
 * --------------------设备启动日志---------------------
 * 1. 活跃设备数
 * 2. 连续活跃设备数
 * 3. 最近连续三周活跃用户数
 * 4. 每日用户留存情况
 * 5. 流失用户数
 * 6. 每日新增设备信息数量
 * 7. 沉默用户数
 * 8. 本周回流用户数
 * --------------------用户操作事件日志------------------
 * 1. 用户评论数据
 * 2. 用户点赞数据
 * 3. 活跃用户数据
 */
public class HiveSQL {
    /**
     * 事件日志DWD 明细数据层，对ODS层进行数据清洗
     */
    public final static String SQL_ODS_TO_DWD_EVENT = "INSERT OVERWRITE TABLE dwd_base_event_log\n" +
            "PARTITION(dt='%s')\n" +
            "SELECT\n" +
            "base_analizer(line,'mid') as mid_id,\n" +
            "base_analizer(line,'uid') as user_id,\n" +
            "base_analizer(line,'vc') as version_code,\n" +
            "base_analizer(line,'vn') as version_name,\n" +
            "base_analizer(line,'l') as lang,\n" +
            "base_analizer(line,'sr') as source,\n" +
            "base_analizer(line,'os') as os,\n" +
            "base_analizer(line,'ar') as area,\n" +
            "base_analizer(line,'md') as model,\n" +
            "base_analizer(line,'ba') as brand,\n" +
            "base_analizer(line,'sv') as sdk_version,\n" +
            "base_analizer(line,'g') as gmail,\n" +
            "base_analizer(line,'hw') as height_width,\n" +
            "base_analizer(line,'t') as app_time,\n" +
            "base_analizer(line,'nw') as network,\n" +
            "base_analizer(line,'ln') as lng,\n" +
            "base_analizer(line,'la') as lat,\n" +
            "event_name,\n" +
            "event_json,\n" +
            "base_analizer(line,'st') as server_time\n" +
            "FROM ods_event_log lateral view flat_analizer(base_analizer(line,'et')) tem_flat as event_name,event_json\n" +
            "WHERE dt='%s'\n" +
            "AND base_analizer(line,'et')<>''";

    /**
     * 事件日志DWD 明细数据层，抽取评论内容
     */
    public final static String SQL_ODS_TO_DWD_COMMENT_EVENT = "INSERT overwrite TABLE dwd_comment_log PARTITION (dt = '%s') SELECT\n" +
            "\tmid_id,\n" +
            "\tuser_id,\n" +
            "\tversion_code,\n" +
            "\tversion_name,\n" +
            "\tlang,\n" +
            "\tsource,\n" +
            "\tos,\n" +
            "\tarea,\n" +
            "\tmodel,\n" +
            "\tbrand,\n" +
            "\tsdk_version,\n" +
            "\tgmail,\n" +
            "\theight_width,\n" +
            "\tapp_time,\n" +
            "\tnetwork,\n" +
            "\tlng,\n" +
            "\tlat,\n" +
            "\tget_json_object(event_json,'$.kv.comment_id') comment_id,\n" +
            "\tget_json_object(event_json,'$.kv.userid') userid,\n" +
            "\tget_json_object(event_json,'$.kv.p_comment_id') p_comment_id,\n" +
            "\tget_json_object(event_json,'$.kv.content') content,\n" +
            "\tget_json_object(event_json,'$.kv.addtime') addtime,\n" +
            "\tget_json_object(event_json,'$.kv.other_id') other_id,\n" +
            "\tget_json_object(event_json,'$.kv.praise_count') praise_count,\n" +
            "\tget_json_object(event_json,'$.kv.reply_count') reply_count,\n" +
            "\tserver_time\n" +
            "FROM\n" +
            "\tdwd_base_event_log\n" +
            "WHERE\n" +
            "\tdt = '%s'\n" +
            "AND event_name = 'comment' ";

    /**
     * 事件日志DWD 明细数据层，抽取点赞数据
     */
    public final static String SQL_ODS_TO_DWD_PRAISE_EVENT = "INSERT overwrite TABLE dwd_praise_log PARTITION (dt = '%s') SELECT\n" +
            "\tmid_id,\n" +
            "\tuser_id,\n" +
            "\tversion_code,\n" +
            "\tversion_name,\n" +
            "\tlang,\n" +
            "\tsource,\n" +
            "\tos,\n" +
            "\tarea,\n" +
            "\tmodel,\n" +
            "\tbrand,\n" +
            "\tsdk_version,\n" +
            "\tgmail,\n" +
            "\theight_width,\n" +
            "\tapp_time,\n" +
            "\tnetwork,\n" +
            "\tlng,\n" +
            "\tlat,\n" +
            "\tget_json_object(event_json,'$.kv.id') id,\n" +
            "\tget_json_object(event_json,'$.kv.userid') userid,\n" +
            "\tget_json_object(event_json,'$.kv.target_id') target_id,\n" +
            "\tget_json_object(event_json,'$.kv.type') type,\n" +
            "\tget_json_object(event_json,'$.kv.add_time') add_time,\n" +
            "\tserver_time\n" +
            "FROM\n" +
            "\tdwd_base_event_log\n" +
            "WHERE\n" +
            "\tdt = '%s'\n" +
            "AND event_name = 'praise' ";

    /**
     * 事件日志DWD 明细数据层，抽取活跃用户数据
     */
    public final static String SQL_ODS_TO_DWD_ACTIVE_EVENT = "INSERT overwrite TABLE dwd_active_background_log PARTITION (dt = '%s') SELECT\n" +
            "\tmid_id,\n" +
            "\tuser_id,\n" +
            "\tversion_code,\n" +
            "\tversion_name,\n" +
            "\tlang,\n" +
            "\tsource,\n" +
            "\tos,\n" +
            "\tarea,\n" +
            "\tmodel,\n" +
            "\tbrand,\n" +
            "\tsdk_version,\n" +
            "\tgmail,\n" +
            "\theight_width,\n" +
            "\tapp_time,\n" +
            "\tnetwork,\n" +
            "\tlng,\n" +
            "\tlat,\n" +
            "\tget_json_object(event_json,'$.kv.active_source') active_source,\n" +
            "\tserver_time\n" +
            "FROM\n" +
            "\tdwd_base_event_log\n" +
            "WHERE\n" +
            "\tdt = '%s'\n" +
            "AND event_name = 'active_background' ";

    /**
     * 启动日志DWD 明细数据层，对ODS层进行数据清洗
     */
    public final static String SQL_ODS_TO_DWD_START = "INSERT OVERWRITE TABLE dwd_start_log\n" +
            "PARTITION (dt='%s')\n" +
            "SELECT\n" +
            "get_json_object(line,'$.mid') mid_id,\n" +
            "get_json_object(line,'$.uid') user_id,\n" +
            "get_json_object(line,'$.vc') version_code,\n" +
            "get_json_object(line,'$.vn') version_name,\n" +
            "get_json_object(line,'$.l') lang,\n" +
            "get_json_object(line,'$.sr') source,\n" +
            "get_json_object(line,'$.os') os,\n" +
            "get_json_object(line,'$.ar') area,\n" +
            "get_json_object(line,'$.md') model,\n" +
            "get_json_object(line,'$.ba') brand,\n" +
            "get_json_object(line,'$.sv') sdk_version,\n" +
            "get_json_object(line,'$.g') gmail,\n" +
            "get_json_object(line,'$.hw') height_width,\n" +
            "get_json_object(line,'$.t') app_time,\n" +
            "get_json_object(line,'$.nw') network,\n" +
            "get_json_object(line,'$.ln') lng,\n" +
            "get_json_object(line,'$.la') lat,\n" +
            "get_json_object(line,'$.entry') entry,\n" +
            "get_json_object(line,'$.open_ad_type') open_ad_type,\n" +
            "get_json_object(line,'$.action') action,\n" +
            "get_json_object(line,'$.loading_time') loading_time,\n" +
            "get_json_object(line,'$.detail') detail,\n" +
            "get_json_object(line,'$.extend1') extend1 FROM ods_start_log WHERE dt='%s'";

    /**
     * 启动日志DWS层 服务数据层，以DWD为基础按天进行轻度汇总
     */
    public final static String SQL_DWD_TO_DWS_START = "INSERT overwrite TABLE dws_uv_detail_daycount PARTITION (dt = '%s') SELECT\n" +
            "\tmid_id,\n" +
            "\tconcat_ws('|', collect_set(user_id)) user_id,\n" +
            "\tconcat_ws(\n" +
            "\t\t'|',\n" +
            "\t\tcollect_set (version_code)\n" +
            "\t) version_code,\n" +
            "\tconcat_ws(\n" +
            "\t\t'|',\n" +
            "\t\tcollect_set (version_name)\n" +
            "\t) version_name,\n" +
            "\tconcat_ws('|', collect_set(lang)) lang,\n" +
            "\tconcat_ws('|', collect_set(source)) source,\n" +
            "\tconcat_ws('|', collect_set(os)) os,\n" +
            "\tconcat_ws('|', collect_set(area)) area,\n" +
            "\tconcat_ws('|', collect_set(model)) model,\n" +
            "\tconcat_ws('|', collect_set(brand)) brand,\n" +
            "\tconcat_ws(\n" +
            "\t\t'|',\n" +
            "\t\tcollect_set (sdk_version)\n" +
            "\t) sdk_version,\n" +
            "\tconcat_ws('|', collect_set(gmail)) gmail,\n" +
            "\tconcat_ws(\n" +
            "\t\t'|',\n" +
            "\t\tcollect_set (height_width)\n" +
            "\t) height_width,\n" +
            "\tconcat_ws('|', collect_set(app_time)) app_time,\n" +
            "\tconcat_ws('|', collect_set(network)) network,\n" +
            "\tconcat_ws('|', collect_set(lng)) lng,\n" +
            "\tconcat_ws('|', collect_set(lat)) lat,\n" +
            "\tcount(*) login_count\n" +
            "FROM\n" +
            "\tdwd_start_log\n" +
            "WHERE\n" +
            "\tdt = '%s'\n" +
            "GROUP BY\n" +
            "\tmid_id ";

    /**
     * DWT层 数据主题层，以DWS层为基础按主题进行汇总
     */
    public final static String SQL_DWS_TO_DWT_START = "INSERT overwrite TABLE dwt_uv_topic SELECT\n" +
            "\tnvl (new.mid_id, old.mid_id),\n" +
            "\tnvl (new.user_id, old.user_id),\n" +
            "\tnvl (\n" +
            "\t\tnew.version_code,\n" +
            "\t\told.version_code\n" +
            "\t),\n" +
            "\tnvl (\n" +
            "\t\tnew.version_name,\n" +
            "\t\told.version_name\n" +
            "\t),\n" +
            "\tnvl (new.lang, old.lang),\n" +
            "\tnvl (new.source, old.source),\n" +
            "\tnvl (new.os, old.os),\n" +
            "\tnvl (new.area, old.area),\n" +
            "\tnvl (new.model, old.model),\n" +
            "\tnvl (new.brand, old.brand),\n" +
            "\tnvl (\n" +
            "\t\tnew.sdk_version,\n" +
            "\t\told.sdk_version\n" +
            "\t),\n" +
            "\tnvl (new.gmail, old.gmail),\n" +
            "\tnvl (\n" +
            "\t\tnew.height_width,\n" +
            "\t\told.height_width\n" +
            "\t),\n" +
            "\tnvl (new.app_time, old.app_time),\n" +
            "\tnvl (new.network, old.network),\n" +
            "\tnvl (new.lng, old.lng),\n" +
            "\tnvl (new.lat, old.lat),\n" +
            "\n" +
            "IF (\n" +
            "\told.mid_id IS NULL,\n" +
            "\t'%s',\n" +
            "\told.login_date_first\n" +
            "),\n" +
            "\n" +
            "IF (\n" +
            "\tnew.mid_id IS NOT NULL,\n" +
            "\t'%s',\n" +
            "\told.login_date_last\n" +
            "),\n" +
            "\n" +
            "IF (\n" +
            "\tnew.mid_id IS NOT NULL,\n" +
            "\tnew.login_count,\n" +
            "\t0\n" +
            "),\n" +
            " nvl (old.login_count, 0) +\n" +
            "IF (new.login_count > 0, 1, 0)\n" +
            "FROM\n" +
            "\t(SELECT * FROM dwt_uv_topic) old\n" +
            "FULL OUTER JOIN (\n" +
            "\tSELECT\n" +
            "\t\t*\n" +
            "\tFROM\n" +
            "\t\tdws_uv_detail_daycount\n" +
            "\tWHERE\n" +
            "\t\tdt = '%s'\n" +
            ") new ON old.mid_id = new.mid_id ";

    /**
     * ADS层 数据应用层 -活跃设备数
     */
    public final static String SQL_DWT_TO_ADS_ACTIVE_DEVICES_START = "INSERT INTO TABLE ads_uv_count SELECT\n" +
            "\t'%s' dt,\n" +
            "\tdaycount.ct,\n" +
            "\twkcount.ct,\n" +
            "\tmncount.ct,\n" +
            "\n" +
            "IF (\n" +
            "\tdate_add(\n" +
            "\t\tnext_day ('%s', 'MO') ,- 1\n" +
            "\t) = '%s',\n" +
            "\t'Y',\n" +
            "\t'N'\n" +
            "),\n" +
            "\n" +
            "IF (\n" +
            "\tlast_day('%s') = '%s',\n" +
            "\t'Y',\n" +
            "\t'N'\n" +
            ")\n" +
            "FROM\n" +
            "\t(\n" +
            "\t\tSELECT\n" +
            "\t\t\t'%s' dt,\n" +
            "\t\t\tcount(*) ct\n" +
            "\t\tFROM\n" +
            "\t\t\tdwt_uv_topic\n" +
            "\t\tWHERE\n" +
            "\t\t\tlogin_date_last = '%s'\n" +
            "\t) daycount\n" +
            "JOIN (\n" +
            "\tSELECT\n" +
            "\t\t'%s' dt,\n" +
            "\t\tcount(*) ct\n" +
            "\tFROM\n" +
            "\t\tdwt_uv_topic\n" +
            "\tWHERE\n" +
            "\t\tlogin_date_last >= date_add(\n" +
            "\t\t\tnext_day ('%s', 'MO') ,- 7\n" +
            "\t\t)\n" +
            "\tAND login_date_last <= date_add(\n" +
            "\t\tnext_day ('%s', 'MO') ,- 1\n" +
            "\t)\n" +
            ") wkcount ON daycount.dt = wkcount.dt\n" +
            "JOIN (\n" +
            "\tSELECT\n" +
            "\t\t'%s' dt,\n" +
            "\t\tcount(*) ct\n" +
            "\tFROM\n" +
            "\t\tdwt_uv_topic\n" +
            "\tWHERE\n" +
            "\t\tdate_format(login_date_last, 'yyyy-MM') = date_format('%s', 'yyyy-MM')\n" +
            ") mncount ON daycount.dt = mncount.dt ";

    /**
     * ADS层 数据应用层 -连续活跃设备数
     */
    public final static String SQL_DWT_TO_ADS_CONTINUOUS_ACTIVE_DEVICES_START = "INSERT INTO TABLE ads_continuity_uv_count SELECT\n" +
            "\t'%s',\n" +
            "\tconcat(\n" +
            "\t\tdate_add('%s' ,- 6),\n" +
            "\t\t'_',\n" +
            "\t\t'%s'\n" +
            "\t),\n" +
            "\tcount(*)\n" +
            "FROM\n" +
            "\t(\n" +
            "\t\tSELECT\n" +
            "\t\t\tmid_id\n" +
            "\t\tFROM\n" +
            "\t\t\t(\n" +
            "\t\t\t\tSELECT\n" +
            "\t\t\t\t\tmid_id\n" +
            "\t\t\t\tFROM\n" +
            "\t\t\t\t\t(\n" +
            "\t\t\t\t\t\tSELECT\n" +
            "\t\t\t\t\t\t\tmid_id,\n" +
            "\t\t\t\t\t\t\tdate_sub(dt, rank) date_dif\n" +
            "\t\t\t\t\t\tFROM\n" +
            "\t\t\t\t\t\t\t(\n" +
            "\t\t\t\t\t\t\t\tSELECT\n" +
            "\t\t\t\t\t\t\t\t\tmid_id,\n" +
            "\t\t\t\t\t\t\t\t\tdt,\n" +
            "\t\t\t\t\t\t\t\t\trank () over (PARTITION BY mid_id ORDER BY dt) rank\n" +
            "\t\t\t\t\t\t\t\tFROM\n" +
            "\t\t\t\t\t\t\t\t\tdws_uv_detail_daycount\n" +
            "\t\t\t\t\t\t\t\tWHERE\n" +
            "\t\t\t\t\t\t\t\t\tdt >= date_add('%s' ,- 6)\n" +
            "\t\t\t\t\t\t\t\tAND dt <= '%s'\n" +
            "\t\t\t\t\t\t\t) t1\n" +
            "\t\t\t\t\t) t2\n" +
            "\t\t\t\tGROUP BY\n" +
            "\t\t\t\t\tmid_id,\n" +
            "\t\t\t\t\tdate_dif\n" +
            "\t\t\t\tHAVING\n" +
            "\t\t\t\t\tcount(*) >= 3\n" +
            "\t\t\t) t3\n" +
            "\t\tGROUP BY\n" +
            "\t\t\tmid_id\n" +
            "\t) t4 ";

    /**
     * ADS层 数据应用层 -最近连续三周活跃用户数
     */
    public final static String SQL_DWT_TO_ADS_THREE_CONSECUTIVE_WEEKS_START = "INSERT INTO TABLE ads_continuity_wk_count SELECT\n" +
            "\t'%s',\n" +
            "\tconcat(\n" +
            "\t\tdate_add(\n" +
            "\t\t\tnext_day ('%s', 'MO') ,- 7 * 3\n" +
            "\t\t),\n" +
            "\t\t'_',\n" +
            "\t\tdate_add(\n" +
            "\t\t\tnext_day ('%s', 'MO') ,- 1\n" +
            "\t\t)\n" +
            "\t),\n" +
            "\tcount(*)\n" +
            "FROM\n" +
            "\t(\n" +
            "\t\tSELECT\n" +
            "\t\t\tmid_id\n" +
            "\t\tFROM\n" +
            "\t\t\t(\n" +
            "\t\t\t\tSELECT mid_id\n" +
            "\t\t\t\tFROM\n" +
            "\t\t\t\t\tdws_uv_detail_daycount\n" +
            "\t\t\t\tWHERE\n" +
            "\t\t\t\t\tdt >= date_add(\n" +
            "\t\t\t\t\t\tnext_day ('%s', 'monday') ,- 7\n" +
            "\t\t\t\t\t)\n" +
            "\t\t\t\tAND dt <= date_add(\n" +
            "\t\t\t\t\tnext_day ('%s', 'monday') ,- 1\n" +
            "\t\t\t\t)\n" +
            "\t\t\t\tGROUP BY\n" +
            "\t\t\t\t\tmid_id\n" +
            "\t\t\t\tUNION ALL\n" +
            "\t\t\t\t\tSELECT\n" +
            "\t\t\t\t\t\tmid_id\n" +
            "\t\t\t\t\tFROM\n" +
            "\t\t\t\t\t\tdws_uv_detail_daycount\n" +
            "\t\t\t\t\tWHERE\n" +
            "\t\t\t\t\t\tdt >= date_add(\n" +
            "\t\t\t\t\t\t\tnext_day ('%s', 'monday') ,- 7 * 2\n" +
            "\t\t\t\t\t\t)\n" +
            "\t\t\t\t\tAND dt <= date_add(\n" +
            "\t\t\t\t\t\tnext_day ('%s', 'monday') ,- 7 - 1\n" +
            "\t\t\t\t\t)\n" +
            "\t\t\t\t\tGROUP BY\n" +
            "\t\t\t\t\t\tmid_id\n" +
            "\t\t\t\t\tUNION ALL\n" +
            "\t\t\t\t\t\tSELECT\n" +
            "\t\t\t\t\t\t\tmid_id\n" +
            "\t\t\t\t\t\tFROM\n" +
            "\t\t\t\t\t\t\tdws_uv_detail_daycount\n" +
            "\t\t\t\t\t\tWHERE\n" +
            "\t\t\t\t\t\t\tdt >= date_add(\n" +
            "\t\t\t\t\t\t\t\tnext_day ('%s', 'monday') ,- 7 * 3\n" +
            "\t\t\t\t\t\t\t)\n" +
            "\t\t\t\t\t\tAND dt <= date_add(\n" +
            "\t\t\t\t\t\t\tnext_day ('%s', 'monday') ,- 7 * 2 - 1\n" +
            "\t\t\t\t\t\t)\n" +
            "\t\t\t\t\t\tGROUP BY\n" +
            "\t\t\t\t\t\t\tmid_id\n" +
            "\t\t\t) t1\n" +
            "\t\tGROUP BY\n" +
            "\t\t\tmid_id\n" +
            "\t\tHAVING\n" +
            "\t\t\tcount(*) = 3\n" +
            "\t) t2 ";

    /**
     * ADS层 数据应用层 -每日用户留存情况
     */
    public final static String SQL_DWT_TO_ADS_DAILY_USER_RETENTION_STATUS_START = "INSERT INTO TABLE ads_user_retention_day_rate SELECT\n" +
            "\t'%s' ,-- 统计日期\n" +
            "\tdate_add('%s' ,- 1) ,-- 新增日期\n" +
            "\t1 ,-- 留存天数\n" +
            "\tsum(\n" +
            "\t\tIF (\n" +
            "\t\t\tlogin_date_first = date_add('%s' ,- 1)\n" +
            "\t\t\tAND login_date_last = '%s',\n" +
            "\t\t\t1,\n" +
            "\t\t\t0\n" +
            "\t\t)\n" +
            "\t) ,-- '%s' 的 1 日留存数\n" +
            "\tsum(\n" +
            "\t\tIF (\n" +
            "\t\t\tlogin_date_first = date_add('%s' ,- 1),\n" +
            "\t\t\t1,\n" +
            "\t\t\t0\n" +
            "\t\t)\n" +
            "\t) ,-- '%s' 新增\n" +
            "\tsum(\n" +
            "\t\tIF (\n" +
            "\t\t\tlogin_date_first = date_add('%s' ,- 1)\n" +
            "\t\t\tAND login_date_last = '%s',\n" +
            "\t\t\t1,\n" +
            "\t\t\t0\n" +
            "\t\t)\n" +
            "\t) / sum(\n" +
            "\n" +
            "\t\tIF (\n" +
            "\t\t\tlogin_date_first = date_add('%s' ,- 1),\n" +
            "\t\t\t1,\n" +
            "\t\t\t0\n" +
            "\t\t)\n" +
            "\t) * 100\n" +
            "FROM\n" +
            "\tdwt_uv_topic UNION ALL SELECT\n" +
            "\t\t'%s' ,-- 统计日期\n" +
            "\t\tdate_add('%s' ,- 2) ,-- 新增日期\n" +
            "\t\t2 ,-- 留存天数\n" +
            "\t\tsum(\n" +
            "\t\t\tIF (\n" +
            "\t\t\t\tlogin_date_first = date_add('%s' ,- 2)\n" +
            "\t\t\t\tAND login_date_last = '%s',\n" +
            "\t\t\t\t1,\n" +
            "\t\t\t\t0\n" +
            "\t\t\t)\n" +
            "\t\t) ,-- '%s' 的 2 日留存数\n" +
            "\t\tsum(\n" +
            "\t\t\tIF (\n" +
            "\t\t\t\tlogin_date_first = date_add('%s' ,- 2),\n" +
            "\t\t\t\t1,\n" +
            "\t\t\t\t0\n" +
            "\t\t\t)\n" +
            "\t\t) ,-- '%s' 新增\n" +
            "\t\tsum(\n" +
            "\t\t\tIF (\n" +
            "\t\t\t\tlogin_date_first = date_add('%s' ,- 2)\n" +
            "\t\t\t\tAND login_date_last = '%s',\n" +
            "\t\t\t\t1,\n" +
            "\t\t\t\t0\n" +
            "\t\t\t)\n" +
            "\t\t) / sum(\n" +
            "\t\t\tIF (\n" +
            "\t\t\t\tlogin_date_first = date_add('%s' ,- 2),\n" +
            "\t\t\t\t1,\n" +
            "\t\t\t\t0\n" +
            "\t\t\t)\n" +
            "\t\t) * 100\n" +
            "\tFROM\n" +
            "\t\tdwt_uv_topic\n" +
            "\tUNION ALL\n" +
            "\t\tSELECT\n" +
            "\t\t\t'%s' ,-- 统计日期\n" +
            "\t\t\tdate_add('%s' ,- 3) ,-- 新增日期\n" +
            "\t\t\t3 ,-- 留存天数\n" +
            "\t\t\tsum(\n" +
            "\t\t\t\tIF (\n" +
            "\t\t\t\t\tlogin_date_first = date_add('%s',- 3)\n" +
            "\t\t\t\t\tAND login_date_last = '%s',\n" +
            "\t\t\t\t\t1,\n" +
            "\t\t\t\t\t0\n" +
            "\t\t\t\t)\n" +
            "\t\t\t) ,-- '%s' 的 3 日留存数\n" +
            "\t\t\tsum(\n" +
            "\t\t\t\tIF (\n" +
            "\t\t\t\t\tlogin_date_first = date_add('%s',- 3),\n" +
            "\t\t\t\t\t1,\n" +
            "\t\t\t\t\t0\n" +
            "\t\t\t\t)\n" +
            "\t\t\t) ,-- '%s' 新增\n" +
            "\t\t\tsum(\n" +
            "\t\t\t\tIF (\n" +
            "\t\t\t\t\tlogin_date_first = date_add('%s' ,- 3)\n" +
            "\t\t\t\t\tAND login_date_last = '%s',\n" +
            "\t\t\t\t\t1,\n" +
            "\t\t\t\t\t0\n" +
            "\t\t\t\t)\n" +
            "\t\t\t) / sum(\n" +
            "\t\t\t\tIF (\n" +
            "\t\t\t\t\tlogin_date_first = date_add('%s' ,- 3),\n" +
            "\t\t\t\t\t1,\n" +
            "\t\t\t\t\t0\n" +
            "\t\t\t\t)\n" +
            "\t\t\t) * 100\n" +
            "\t\tFROM\n" +
            "\t\t\tdwt_uv_topic ";

    /**
     * ADS层 数据应用层 -流失用户数
     */
    public final static String SQL_DWT_TO_ADS_LOST_USERS_START = "INSERT INTO TABLE ads_wastage_count SELECT\n" +
            "\t'%s',\n" +
            "\tcount(*)\n" +
            "FROM\n" +
            "\t(\n" +
            "\t\tSELECT\n" +
            "\t\t\tmid_id\n" +
            "\t\tFROM\n" +
            "\t\t\tdwt_uv_topic\n" +
            "\t\tWHERE\n" +
            "\t\t\tlogin_date_last <= date_add('%s' ,- 7)\n" +
            "\t\tGROUP BY\n" +
            "\t\t\tmid_id\n" +
            "\t) t1 ";

    /**
     * ADS层 数据应用层 -每日新增设备信息数量
     */
    public final static String SQL_DWT_TO_ADS_NUMBER_OF_NEW_DEVICE_ADDED_DAILY_START = "INSERT INTO TABLE ads_new_mid_count SELECT\n" +
            "\tlogin_date_first,\n" +
            "\tcount(*)\n" +
            "FROM\n" +
            "\tdwt_uv_topic\n" +
            "WHERE\n" +
            "\tlogin_date_first = '%s'\n" +
            "GROUP BY\n" +
            "\tlogin_date_first ";

    /**
     * ADS层 数据应用层 -沉默用户数
     */
    public final static String SQL_DWT_TO_ADS_NUMBER_OF_SILENT_USERS_START = "INSERT INTO TABLE ads_silent_count SELECT\n" +
            "\t'%s',\n" +
            "\tcount(*)\n" +
            "FROM\n" +
            "\tdwt_uv_topic\n" +
            "WHERE\n" +
            "\tlogin_date_first = login_date_last\n" +
            "AND login_date_last <= date_add('%s' ,- 7) ";

    /**
     * ADS层 数据应用层 -本周回流用户数
     */
    public final static String SQL_DWT_TO_ADS_RETURNING_USERS_START = "INSERT INTO TABLE ads_back_count SELECT\n" +
            "\t'%s',\n" +
            "\tcount(*)\n" +
            "FROM\n" +
            "\t(\n" +
            "\t\tSELECT\n" +
            "\t\t\tmid_id\n" +
            "\t\tFROM\n" +
            "\t\t\tdwt_uv_topic\n" +
            "\t\tWHERE\n" +
            "\t\t\tlogin_date_last >= date_add(\n" +
            "\t\t\t\tnext_day ('%s', 'MO') ,- 7\n" +
            "\t\t\t)\n" +
            "\t\tAND login_date_last <= date_add(\n" +
            "\t\t\tnext_day ('%s', 'MO') ,- 1\n" +
            "\t\t)\n" +
            "\t\tAND login_date_first < date_add(\n" +
            "\t\t\tnext_day ('%s', 'MO') ,- 7\n" +
            "\t\t)\n" +
            "\t) current_wk\n" +
            "LEFT JOIN (\n" +
            "\tSELECT\n" +
            "\t\tmid_id\n" +
            "\tFROM\n" +
            "\t\tdws_uv_detail_daycount\n" +
            "\tWHERE\n" +
            "\t\tdt >= date_add(\n" +
            "\t\t\tnext_day ('%s', 'MO') ,- 7 * 2\n" +
            "\t\t)\n" +
            "\tAND dt <= date_add(\n" +
            "\t\tnext_day ('%s', 'MO') ,- 7 - 1\n" +
            "\t)\n" +
            "\tGROUP BY\n" +
            "\t\tmid_id\n" +
            ") last_wk ON current_wk.mid_id = last_wk.mid_id\n" +
            "WHERE\n" +
            "\tlast_wk.mid_id IS NULL ";
}
