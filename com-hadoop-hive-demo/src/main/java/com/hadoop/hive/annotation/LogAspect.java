package com.hadoop.hive.annotation;

import java.lang.annotation.*;

/**
 * 日志注解,用于指定要记录日志的方法或类。
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@Documented
public @interface LogAspect {
    /**
     * 标识日志内容
     *
     * @return
     */
    String value() default "";

    /**
     * 是否记录日志
     *
     * @return
     */
    boolean isWriteLog() default true;
}
