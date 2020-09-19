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
     * 用户日志流式处理主题
     */
    public static String USER_LOG_PROCESSOR_TOPIC_FROM = "kafka_topic_log_processor_from";

    /**
     * 用户日志流式处理主题
     */
    public static String USER_LOG_PROCESSOR_TOPIC_TO = "kafka_topic_log_processor_to";
}
