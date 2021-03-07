package com.hadoop.flink.operator;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

import javax.annotation.Nullable;

/**
 * Join: join操作很常见，与我们数据库中常见的inner join类似，它数据的数据侧重与pair，它会按照一定的条件取出两个流或者数据集中匹配的数据返回给下游处理或者输出。
 * Join操作DataStream时只能用在window中，和cogroup操作一样。
 * 它的编程模型如下：
 * stream.join(otherStream)
 * .where(<KeySelector>)
 * .equalTo(<KeySelector>)
 * .window(<WindowAssigner>)
 * .apply(<JoinFunction>)
 */
public class DataStreamJoinOperator {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<String, String>> input1 = env.socketTextStream("192.168.217.110", 9002)
                .map(new MapFunction<String, Tuple2<String, String>>() {

                    @Override
                    public Tuple2<String, String> map(String s) throws Exception {

                        return Tuple2.of(s.split(" ")[0], s.split(" ")[1]);
                    }
                }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, String>>() {
                    private long max = 2000;
                    private long currentTime;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentTime - max);
                    }

                    @Override
                    public long extractTimestamp(Tuple2<String, String> element, long event) {
                        long timestamp = event;
                        currentTime = Math.max(timestamp, currentTime);
                        return currentTime;
                    }
                });

        DataStream<Tuple2<String, String>> input2 = env.socketTextStream("192.168.217.110", 9001)
                .map(new MapFunction<String, Tuple2<String, String>>() {

                    @Override
                    public Tuple2<String, String> map(String s) throws Exception {

                        return Tuple2.of(s.split(" ")[0], s.split(" ")[1]);
                    }
                }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, String>>() {
                    private long max = 5000;
                    private long currentTime;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(System.currentTimeMillis() - max);
                    }

                    @Override
                    public long extractTimestamp(Tuple2<String, String> element, long event) {
                        long timestamp = event;
                        currentTime = Math.max(timestamp, currentTime);
                        return currentTime;
                    }
                });

        input1.join(input2)
                .where(new KeySelector<Tuple2<String, String>, Object>() {

                    @Override
                    public Object getKey(Tuple2<String, String> t) throws Exception {
                        return t.f0;
                    }
                }).equalTo(new KeySelector<Tuple2<String, String>, Object>() {

            @Override
            public Object getKey(Tuple2<String, String> T) throws Exception {
                return T.f0;
            }
        })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(CountTrigger.of(1))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Object>() {

                    @Override
                    public Object join(Tuple2<String, String> tuple1, Tuple2<String, String> tuple2) throws Exception {
                        if (tuple1.f0.equals(tuple2.f0)) {
                            return tuple1.f1 + " " + tuple2.f1;
                        }
                        return null;
                    }
                }).print();

        env.execute();

    }
}
