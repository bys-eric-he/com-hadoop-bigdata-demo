package com.hadoop.flink.operator;

import com.hadoop.flink.pojo.UserAction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Fliter: 过滤出需要的数据
 */
public class DataStreamFilterOperator {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 输入: 用户行为。某个用户在某个时刻点击或浏览了某个商品，以及商品的价格。
        DataStreamSource<UserAction> source = env.fromCollection(Arrays.asList(
                new UserAction("userID1", 1293984000L, "click", "productID1", 10.0),
                new UserAction("userID2", 1293984001L, "browse", "productID2", 8.0),
                new UserAction("userID1", 1293984002L, "click", "productID1", 10.0)
        ));

        // 过滤: 过滤出用户ID为userID1的用户行为
        SingleOutputStreamOperator<UserAction> result = source.filter(new FilterFunction<UserAction>() {
            @Override
            public boolean filter(UserAction value) throws Exception {
                return value.getUserID().equals("userID1");
            }
        });

        // 输出: 输出到控制台
        // UserAction(userID=userID1, eventTime=1293984002, eventType=click, productID=productID1, productPrice=10)
        // UserAction(userID=userID1, eventTime=1293984000, eventType=click, productID=productID1, productPrice=10)
        result.print("----------Filter-----------");

        env.execute();

    }
}
