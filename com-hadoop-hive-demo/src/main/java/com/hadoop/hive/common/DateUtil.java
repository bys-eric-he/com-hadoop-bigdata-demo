package com.hadoop.hive.common;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateUtil {

    /**
     * 默认日期时间格式
     */
    public static final String DEFAULT_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    /**
     * 默认日期格式
     */
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    /**
     * 默认时间格式
     */
    public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss";

    /**
     * 获取当前日期格式化字符串
     *
     * @param "yyyy-MM-dd"
     * @return
     */
    public static String getCurrentDateString(String format) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern(format);
        return dtf.format(LocalDateTime.now());
    }

    /**
     * 获取当前日期对象
     *
     * @return
     */
    public static LocalDateTime getCurrentDateTime() {
        return LocalDateTime.now();
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

    /**
     * 获取LocalDateTime对象
     *
     * @param datetime
     * @param format   "yyyy-MM-dd HH:mm:ss"
     * @return
     */
    public static LocalDateTime getLocalDateTimeByString(String datetime, String format) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern(format);
        return LocalDateTime.parse(datetime, dtf);
    }
}
