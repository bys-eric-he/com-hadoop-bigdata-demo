package com.hadoop.kafka.event.listener;

import com.hadoop.kafka.event.model.UserOrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;

/**
 * 监听配置 使用@EventListener方式
 */
@Configuration
@Slf4j
public class UserOrderListenerConfig {
    /*
    @EventListener
    public void handleEvent(Object event) {
        //监听所有事件 可以看看 系统各类时间 发布了哪些事件
        //可根据 instanceof 监听想要监听的事件
        if (event instanceof UserOrderEvent) {
            log.info("---->监听到UserOrderEvent事件：{}", event);
        } else {
            log.info("---->事件：{}", event);
        }
    }*/

    /**
     * 异步监听
     *
     * @param userOrderEvent
     */
    @Async
    @EventListener
    public void handleCustomEvent(UserOrderEvent userOrderEvent) {
        //监听UserOrderEvent事件
        log.info("---->异步监听听到UserOrderEvent事件，消息为：{}, 发布时间：{}", userOrderEvent.getUserOrder(), userOrderEvent.getTimestamp());
    }



    /*
     * 监听 orderID为202100001的事件
     *
    @EventListener(condition = "#userOrderEvent.userOrder.orderID == '202100001'")
    public void handleCustomEventByCondition(UserOrderEvent userOrderEvent) {
        //监听UserOrderEvent事件
        log.info("---->监听到orderID为'202100001'的userOrderEvent事件, 消息为：{}, 发布时间：{}", userOrderEvent.getUserOrder(), userOrderEvent.getTimestamp());
    }*/
}
