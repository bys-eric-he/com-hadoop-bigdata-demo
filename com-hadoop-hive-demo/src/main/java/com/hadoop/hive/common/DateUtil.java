package com.hadoop.hive.common;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateUtil {

    /** 默认日期时间格式 */
    public static final String DEFAULT_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    /** 默认日期格式 */
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    /** 默认时间格式 */
    public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss";

    /**
     * 获取当前日期
     *
     * @param "yyyy-MM-dd"
     * @return
     */
    public static String getCurrentDate(String format) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern(format);
        return dtf.format(LocalDateTime.now());
    }

    /**
     * 获取LocalDateTime字符串
     *
     * @param dateTime
     * @param format   "yyyy-MM-dd HH:mm:ss"
     * @return
     */
    public static String getLocalDateTime(LocalDateTime dateTime, String format) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern(format);
        return dtf.format(dateTime);
    }
}
