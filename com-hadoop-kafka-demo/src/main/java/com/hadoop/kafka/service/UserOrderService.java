package com.hadoop.kafka.service;

import com.hadoop.kafka.model.UserOrder;

import java.util.List;

/**
 * 用户订单服务
 */
public interface UserOrderService {
    /**
     * 插入用户订单
     *
     * @param userOrder
     * @return
     */
    int upInsertUserOrder(UserOrder userOrder) throws Exception;

    /**
     * 删除指定订单
     *
     * @param id
     * @return
     */
    int deleteUserOrder(String id);

    /**
     * 获取指定订单信息
     *
     * @param id
     * @return
     */
    UserOrder getOrderById(String id);

    /**
     * 获取指定用户订单
     *
     * @param userId
     * @return
     */
    List<UserOrder> getOrdersByUserId(String userId);

    /**
     * 获取所有订单信息
     *
     * @return
     */
    List<UserOrder> getAllOrders();
}
