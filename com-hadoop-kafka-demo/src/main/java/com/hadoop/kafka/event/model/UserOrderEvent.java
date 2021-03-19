package com.hadoop.kafka.event.model;

import com.hadoop.kafka.model.UserOrder;
import org.springframework.context.ApplicationEvent;

/**
 * 用户订单事件源
 */
public class UserOrderEvent extends ApplicationEvent {
    private static final long serialVersionUID = 1L;
    private UserOrder userOrder;

    public UserOrderEvent(Object source, UserOrder userOrder) {
        super(source);
        this.userOrder = userOrder;
    }

    public UserOrder getUserOrder() {
        return userOrder;
    }

    public void setUserOrder(UserOrder userOrder) {
        this.userOrder = userOrder;
    }
}
