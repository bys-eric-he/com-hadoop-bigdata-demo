package com.hadoop.flink.operator;

import com.hadoop.flink.pojo.UserAction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Reduce: 基于ReduceFunction进行滚动聚合，并向下游算子输出每次滚动聚合后的结果。
 */
public class DataStreamReduceOperator {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 输入: 用户行为。某个用户在某个时刻点击或浏览了某个商品，以及商品的价格。
        DataStreamSource<UserAction> source = env.fromCollection(Arrays.asList(
                new UserAction("userID1", 1293984000L, "click", "productID1", 10.0),
                new UserAction("userID2", 1293984001L, "browse", "productID2", 8.0),
                new UserAction("userID2", 1293984002L, "browse", "productID2", 8.0),
                new UserAction("userID2", 1293984003L, "browse", "productID2", 8.0),
                new UserAction("userID1", 1293984002L, "click", "productID1", 10.0),
                new UserAction("userID1", 1293984003L, "click", "productID3", 10.0),
                new UserAction("userID1", 1293984004L, "click", "productID1", 10.0)
        ));

        // 转换: KeyBy对数据重分区
        KeyedStream<UserAction, String> keyedStream = source.keyBy(new KeySelector<UserAction, String>() {
            @Override
            public String getKey(UserAction value) throws Exception {
                return value.getUserID();
            }
        });

        // 转换: Reduce滚动聚合。这里滚动聚合每个用户对应的商品总价格。
        SingleOutputStreamOperator<UserAction> result = keyedStream.reduce(new ReduceFunction<UserAction>() {
            @Override
            public UserAction reduce(UserAction value1, UserAction value2) throws Exception {
                double newProductPrice = value1.getProductPrice() + value2.getProductPrice();
                return new UserAction(value1.getUserID(), -1L, "", "", newProductPrice);
            }
        });

        // 输出: 将每次滚动聚合后的结果输出到控制台。
        //3> UserAction(userID=userID2, eventTime=1293984001, eventType=browse, productID=productID2, productPrice=8)
        //3> UserAction(userID=userID2, eventTime=-1, eventType=, productID=, productPrice=16)
        //3> UserAction(userID=userID2, eventTime=-1, eventType=, productID=, productPrice=24)
        //4> UserAction(userID=userID1, eventTime=1293984000, eventType=click, productID=productID1, productPrice=10)
        //4> UserAction(userID=userID1, eventTime=-1, eventType=, productID=, productPrice=20)
        //4> UserAction(userID=userID1, eventTime=-1, eventType=, productID=, productPrice=30)
        //4> UserAction(userID=userID1, eventTime=-1, eventType=, productID=, productPrice=40)
        result.print();

        env.execute();
    }
}
