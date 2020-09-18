package com.hadoop.kafka.common;

/**
 * 常量类
 */
public class TopicConstant {
    /**
     * 用户日志消息主题
     */
    public static String USER_LOG_TOPIC_MESSAGE = "kafka_topic_user_log_message";

    /**
     * 用户订单消息主题
     */
    public static String USER_ORDER_TOPIC_MESSAGE = "kafka_topic_user_order_message";

    /**
     * 用户订单主题分区1
     */
    public static String USER_ORDER_KEY_1 = "1";

    /**
     * 用户订单主题分区2
     */
    public static String USER_ORDER_KEY_2 = "2";
}
