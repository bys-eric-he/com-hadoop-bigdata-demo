package com.hadoop.kafka.mapper;

import com.hadoop.kafka.entity.UserOrderEntity;
import org.mapstruct.Mapper;

import java.util.List;

@Mapper
public interface UserOrderMapper {
    /**
     * 插入用户订单
     *
     * @param entity
     * @return
     */
    int upInsertUserOrder(UserOrderEntity entity);

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
    UserOrderEntity getOrderById(String id);

    /**
     * 获取指定用户订单
     *
     * @param userId
     * @return
     */
    List<UserOrderEntity> getOrdersByUserId(String userId);

    /**
     * 获取所有订单信息
     *
     * @return
     */
    List<UserOrderEntity> getAllOrders();
}
