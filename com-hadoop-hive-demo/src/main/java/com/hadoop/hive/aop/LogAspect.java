package com.hadoop.hive.aop;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Service;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * 日志切面
 */
@Aspect
@Service
@Slf4j
public class LogAspect {
    /**
     * 定义拦截规则：拦截所有@LogAspect注解的方法。
     */
    @Pointcut("@annotation(com.hadoop.hive.annotation.LogAspect)")
    public void logPointcut() {
    }

    /**
     * 拦截器具体实现
     *
     * @param joinPoint
     * @return
     * @throws Throwable
     */
    @Around("logPointcut()")
    public Object interceptor(ProceedingJoinPoint joinPoint) throws Throwable {
        long beginTime = System.currentTimeMillis();
        log.info("------- 进入 AOP 日志切面处理 ------- ");
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        //获取被拦截的方法
        Method method = signature.getMethod();
        //获取方法注解
        com.hadoop.hive.annotation.LogAspect logAspect = method.getAnnotation(com.hadoop.hive.annotation.LogAspect.class);

        String[] parameterNames = signature.getParameterNames();

        if (logAspect != null && logAspect.isWriteLog()) {
            log.info("方法参数名称 -> " + Arrays.toString(parameterNames));
            log.info("方法参数值 -> " + Arrays.toString(joinPoint.getArgs()));
        }

        log.info("-------- 结束 AOP 日志切面处理 ------- 耗时：" + (System.currentTimeMillis() - beginTime));

        return joinPoint.proceed();
    }
}