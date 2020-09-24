package com.hadoop.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * UDF 用于解析公共字段
 */
public class BaseFieldUDF extends UDF {
    public String evaluate(String line, String key) throws JSONException {
        String[] log = line.split("%");
        if (log.length != 2 || StringUtils.isBlank(log[1])) {
            return "";
        }
        JSONObject baseJson = new JSONObject(log[1].trim());
        String result = "";
        // 获取服务器时间
        if ("st".equals(key)) {
            result = log[0].trim();
        } else if ("et".equals(key)) {
            // 获取事件数组
            if (baseJson.has("et")) {
                result = baseJson.getString("et");
            }
        } else {
            JSONObject cm = baseJson.getJSONObject("cm");
            // 获取 key 对应公共字段的 value
            if (cm.has(key)) {
                result = cm.getString(key);
            }
        }
        return result;
    }

    public static void main(String[] args) throws JSONException {
        String line = "2020-09-24 14:59:56 236%{\"cm\":{\"ln\":\"-48.5\",\"sv\":\"V2.5.7\",\"os\":\"8.0.9\",\"g\":\"6F76AVD5@gmail.com\",\"mid\":\"M00003\",\"nw\":\"4G\",\"l\":\"pt\",\"vc\":\"3\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"202000005\",\"t\":\"1583707297317\",\"la\":\"-52.9\",\"md\":\"Sumsung-18\",\"vn\":\"1.2.4\",\"ba\":\"Sumsung\",\"sr\":\"V\"},\"ap\":\"app \",\"et\":[{\"ett\":\"1583705574227\",\"en\":\"display\",\"kv\":{\"goodsid\":\"0\",\"action\":\"1\",\"extend1\":\"1\",\"place\":\"0\",\"category\":\"63\"}},{\"ett\":\"1583760986259\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"4\",\"action\":\"3\",\"extend1\":\"\",\"type\":\"3\",\"type1\":\"\",\"loading_way\":\"1 \"}},{\"ett\":\"1583746639124\",\"en\":\"ad\",\"kv\":{\"activityId\":\"1\",\"displa yMills\":\"111839\",\"entry\":\"1\",\"action\":\"5\",\"contentType\":\"0\"}},{\"ett \":\"1583758016208\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1583694079866\",\"action\":\"1\",\"type\":\"3\",\"content\":\"\"}},{\"ett\":\"1583699890760\",\"en \":\"favorites\",\"kv\":{\"course_id\":4,\"id\":0,\"add_time\":\"1583730648134\",\"u serid\":7}}]}";

        String
        mid = new BaseFieldUDF().evaluate(line, "mid");
        System.out.println(mid);
        mid = new BaseFieldUDF().evaluate(line, "uid");
        System.out.println(mid);
        mid = new BaseFieldUDF().evaluate(line, "vc");
        System.out.println(mid);
        mid = new BaseFieldUDF().evaluate(line, "vn");
        System.out.println(mid);
        mid = new BaseFieldUDF().evaluate(line, "l");
        System.out.println(mid);
        mid = new BaseFieldUDF().evaluate(line, "sr");
        System.out.println(mid);
        mid = new BaseFieldUDF().evaluate(line, "os");
        System.out.println(mid);
        mid = new BaseFieldUDF().evaluate(line, "ar");
        System.out.println(mid);
        mid = new BaseFieldUDF().evaluate(line, "md");
        System.out.println(mid);
        mid = new BaseFieldUDF().evaluate(line, "ba");
        System.out.println(mid);
        mid = new BaseFieldUDF().evaluate(line, "sv");
        System.out.println(mid);
        mid = new BaseFieldUDF().evaluate(line, "g");
        System.out.println(mid);
        mid = new BaseFieldUDF().evaluate(line, "hw");
        System.out.println(mid);
        mid = new BaseFieldUDF().evaluate(line, "t");
        System.out.println(mid);
        mid = new BaseFieldUDF().evaluate(line, "nw");
        System.out.println(mid);
        mid = new BaseFieldUDF().evaluate(line, "ln");
        System.out.println(mid);
        mid = new BaseFieldUDF().evaluate(line, "la");
        System.out.println(mid);
        mid = new BaseFieldUDF().evaluate(line, "st");
        System.out.println(mid);
    }
}