/*------------------创建数据库--------------------*/
CREATE DATABASE demo_database_gmall;

USE demo_database_gmall;

/*-------------------ODS 原始数据层，存放原始数据（用户行为数据）-----------------*/

ALTER TABLE ods_start_log
    SET TBLPROPERTIES ('EXTERNAL' = 'FALSE');

DROP TABLE
    IF EXISTS ods_start_log;

CREATE EXTERNAL TABLE ods_start_log
(
    `line` STRING
)
    COMMENT '启动日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_start_log';

/*-----插入语句------*/
INSERT OVERWRITE TABLE ods_start_log PARTITION (dt = '2020-09-25')
SELECT '{"mid":"M0001","uid":"2020092409590001","vc":"V 2.3.1","vn":"Beta","l":"CHN","sr":"C","os":"8.6","ar":"shen zhen","md":"Plus","ba":"Apple","sv":"V 1.5.6","g":"heyong@gmail.com","hw":"20*10","t":"2020-09-25 09:35:00","nw":"4G","ln":"-63.8","la":"-0.69999999","entry":"App","open_ad_type":"Web H5","action":"login","loading_time":"202009241038521","detail":"The log info is belongs to user start app....","extend1":{"data":"START APP LOG","date":"2020-09-25"}}';


ALTER TABLE ods_event_log
    SET TBLPROPERTIES ('EXTERNAL' = 'FALSE');

DROP TABLE
    IF EXISTS ods_event_log;

CREATE EXTERNAL TABLE ods_event_log
(
    `line` STRING
)
    COMMENT '事件日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_event_log';

/*-----插入语句------*/
INSERT OVERWRITE TABLE ods_event_log PARTITION (dt = '2020-09-23')
SELECT '2020-09-23 12:36:09%{"cm":{"ln":"-26.5","sv":"V2.5.7","os":"14.0","g":"sky@gmail.com","mid":"M00002","nw":"4G","l":"pt","vc":"3","hw":"560*1023","ar":"MX","uid":"2020092300001","t":"1583707297317","la":"-42.3","md":"iPhone","vn":"Xs","ba":"Apple","sr":"V"},"ap":"app ","et":[{"ett":"1583705574288","en":"active_background","kv":{"active_source":"Mobile-App"}},{"ett":"1583705574227","en":"praise","kv":{"id":"20200001","userid":"2020092300001","target_id":"T-00001","type":"T-01","add_time":"2020-09-25 11:01:00"}},{"ett":"1583760986260","en":"comment","kv":{"comment_id":"1000255563202","userid":"2020092300001","p_comment_id":"1100225320004","content":"I want to tell you, the notebook is on my desk!!","addtime":"2020-09-23 11:26:00","other_id":"110023652200005","praise_count":"11 ","reply_count":"22"}},{"ett":"1583760986259","en":"comment","kv":{"comment_id":"1000255563201","userid":"2020092300001","p_comment_id":"1100225320003","content":"Today is GOOD Day！！！","addtime":"2020-09-23 11:13:00","other_id":"110023652200004","praise_count":"15 ","reply_count":"28"}},{"ett":"1583760986261","en":"comment","kv":{"comment_id":"1000255563203","userid":"2020092300001","p_comment_id":"1100225320005","content":"May be LOVE is joke, LIKE is right!!!","addtime":"2020-09-23 11:58:00","other_id":"110023652200006","praise_count":"9 ","reply_count":"3"}}]}';


/*修改表注释*/
ALTER TABLE ods_event_log
    SET
        TBLPROPERTIES ('comment' = 'Event log table');