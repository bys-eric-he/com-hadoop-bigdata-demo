package com.hadoop.flink.operator;

import com.hadoop.flink.pojo.UserAction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Arrays;
import java.util.Random;

/**
 * Reduce: 基于ReduceFunction进行滚动聚合，并向下游算子输出每次滚动聚合后的结果。
 */
public class DataStreamReduceOperator {

    /**
     * 实现了一个模拟的数据源，它继承自 RichParallelSourceFunction
     */
    private static class DataSource extends RichParallelSourceFunction<UserAction> {
        private volatile boolean isRunning = true;

        public void run(SourceContext<UserAction> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1) * 1000 * 5);
                String userID = "userID-" + (char) ('A' + random.nextInt(10));
                String productID = "productID-" + (char) ('A' + random.nextInt(26));
                Double price = random.nextDouble();

                System.out.println(String.format("订单信息->\t(用户：%s, 订单号：%s, 价格：%f)", userID, productID, price));

                UserAction userAction = new UserAction(userID, System.currentTimeMillis(), "click", productID, price);
                ctx.collect(userAction);
            }
        }

        public void cancel() {
            isRunning = false;
        }
    }

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

        //结构：类别、成交量
        DataStreamSource<UserAction> ds = env.addSource(new DataSource());

        //静态数据源转换: KeyBy对数据重分区
        /*KeyedStream<UserAction, String> keyedStream = source.keyBy(new KeySelector<UserAction, String>() {
            @Override
            public String getKey(UserAction value) throws Exception {
                return value.getUserID();
            }
        });*/

        //动态数据源转换: KeyBy对数据重分区
        KeyedStream<UserAction, String> keyedStream = ds.keyBy(new KeySelector<UserAction, String>() {
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
                return new UserAction(value1.getUserID() + "#" + value2.getUserID(), -1L,
                        value1.getEventType() + "&" + value2.getEventType(),
                        value1.getProductID() + "&" + value2.getProductID(),
                        newProductPrice);
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
