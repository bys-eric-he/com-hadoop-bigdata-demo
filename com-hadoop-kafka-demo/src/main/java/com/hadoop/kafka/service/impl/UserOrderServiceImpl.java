package com.hadoop.kafka.service.impl;

import com.hadoop.kafka.common.Dto2EntityUntil;
import com.hadoop.kafka.entity.UserOrderEntity;
import com.hadoop.kafka.mapper.UserOrderMapper;
import com.hadoop.kafka.model.UserOrder;
import com.hadoop.kafka.service.UserOrderService;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service("userOrderService")
public class UserOrderServiceImpl implements UserOrderService {

    @Resource
    private UserOrderMapper userOrderMapper;

    private ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 插入用户订单
     *
     * @param userOrder
     * @return
     */
    @Override
    public int upInsertUserOrder(UserOrder userOrder) throws Exception{
        UserOrderEntity entity = new UserOrderEntity();
        Dto2EntityUntil.copyProperties(userOrder, entity);
        log.info("-----------------持久化消息到用户订单表-------------------");
        log.info(objectMapper.writeValueAsString(entity));
        return userOrderMapper.upInsertUserOrder(entity);
    }

    /**
     * 删除指定订单
     *
     * @param id
     * @return
     */
    @Override
    public int deleteUserOrder(String id) {
        return userOrderMapper.deleteUserOrder(id);
    }

    /**
     * 获取指定订单信息
     *
     * @param id
     * @return
     */
    @Override
    public UserOrder getOrderById(String id) {
        UserOrderEntity entity = userOrderMapper.getOrderById(id);
        UserOrder result = new UserOrder();
        Dto2EntityUntil.copyProperties(entity, result);
        return result;
    }

    /**
     * 获取指定用户订单
     *
     * @param userId
     * @return
     */
    @Override
    public List<UserOrder> getOrdersByUserId(String userId) {
        List<UserOrderEntity> userOrderEntities = userOrderMapper.getOrdersByUserId(userId);
        List<UserOrder> userOrders = new ArrayList<>();
        userOrderEntities.forEach(o -> {
            UserOrder result = new UserOrder();
            Dto2EntityUntil.copyProperties(o, result);
            userOrders.add(result);
        });
        return userOrders;
    }

    /**
     * 获取所有订单信息
     *
     * @return
     */
    @Override
    public List<UserOrder> getAllOrders() {
        List<UserOrderEntity> userOrderEntities = userOrderMapper.getAllOrders();
        List<UserOrder> userOrders = new ArrayList<>();
        userOrderEntities.forEach(o -> {
            UserOrder result = new UserOrder();
            Dto2EntityUntil.copyProperties(o, result);
            userOrders.add(result);
        });
        return userOrders;
    }
}
