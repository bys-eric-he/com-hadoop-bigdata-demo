package com.hadoop.kafka.event.publisher;

import com.hadoop.kafka.event.model.UserOrderEvent;
import com.hadoop.kafka.model.UserOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class UserOrderPublisher {
    /**
     * 注入 AppllcationContext用来发布事件
     */
    @Autowired
    private ApplicationContext applicationContext;

    /**
     * 使用 AppllicationContext的 publishEvent方法来发布
     *
     * @param userOrder
     */
    public void publish(UserOrder userOrder) {
        applicationContext.publishEvent(new UserOrderEvent(this, userOrder));
    }
}
