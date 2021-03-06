package com.hadoop.flink.operator;

import com.hadoop.flink.pojo.UserAction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Map: 一对一转换,即一条转换成另一条。
 */
@Slf4j
public class DataStreamMapOperator {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 输入: 用户行为。某个用户在某个时刻点击或浏览了某个商品，以及商品的价格。
        DataStreamSource<UserAction> source = env.fromCollection(Arrays.asList(
                new UserAction("userID1", 1293984000L, "click", "productID1", 10.0),
                new UserAction("userID2", 1293984001L, "browse", "productID2", 8.0),
                new UserAction("userID1", 1293984002L, "click", "productID1", 10.0)
        ));

        // 转换: 商品的价格乘以8
        SingleOutputStreamOperator<UserAction> result = source.map(new MapFunction<UserAction, UserAction>() {
            @Override
            public UserAction map(UserAction value) throws Exception {

                double newPrice = value.getProductPrice() * 8;
                return new UserAction(value.getUserID(), value.getEventTime(), value.getEventType(), value.getProductID(), newPrice);
            }
        });

        // 输出: 输出到控制台
        // UserAction(userID=userID1, eventTime=1293984002, eventType=click, productID=productID1, productPrice=80)
        // UserAction(userID=userID1, eventTime=1293984000, eventType=click, productID=productID1, productPrice=80)
        // UserAction(userID=userID2, eventTime=1293984001, eventType=browse, productID=productID2, productPrice=64)
        result.print("----Map----");

        env.execute();
    }
}
