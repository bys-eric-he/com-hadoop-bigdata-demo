package com.hadoop.kafka.event.listener;

import com.hadoop.kafka.event.model.UserOrderEvent;
import com.hadoop.kafka.model.UserOrder;
import com.hadoop.kafka.service.UserOrderService;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

/**
 * 事件监听器 继承ApplicationListener接口 方式
 * 实现 ApplicationListener接口, 并指定监听的事件类型
 */
@Slf4j
@Component
public class UserOrderListener implements ApplicationListener<UserOrderEvent> {

    @Autowired
    @Qualifier("userOrderService")
    private UserOrderService userOrderService;

    private ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 事件处理
     *
     * @param userOrderEvent 使用 onApplicationEvent方法对消息进行接受处理
     */
    @Override
    public void onApplicationEvent(UserOrderEvent userOrderEvent) {
        try {
            UserOrder userOrder = userOrderEvent.getUserOrder();
            log.info("----->我 (UserOrderListener) 接收到了 UserOrderPublisher 发布的消息:"
                    + objectMapper.writeValueAsString(userOrder) + "发布时间：" + userOrderEvent.getTimestamp());
            userOrderService.upInsertUserOrder(userOrder);
        } catch (Exception ex) {
            log.error("***************事件监听器处理异常********************" + ex.getMessage());
        }
    }
}
