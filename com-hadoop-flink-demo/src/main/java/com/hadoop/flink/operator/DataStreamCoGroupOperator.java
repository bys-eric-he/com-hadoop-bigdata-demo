package com.hadoop.flink.operator;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;

/**
 * CoGroup: 该操作是将两个数据流/集合按照key进行group，然后将相同key的数据进行处理，但是它和join操作稍有区别，它在一个流/数据集中没有找到与另一个匹配的数据还是会输出。
 * 在DataStream中:
 * 侧重与group，对同一个key上的两组集合进行操作。
 * 如果在一个流中没有找到与另一个流的window中匹配的数据，任何输出结果，即只输出一个流的数据。
 * 仅能使用在window中。
 */
public class DataStreamCoGroupOperator {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, String>> input1 = env.socketTextStream("192.168.217.110", 9002)
                .map(new MapFunction<String, Tuple2<String, String>>() {

                    @Override
                    public Tuple2<String, String> map(String s) throws Exception {
                        return Tuple2.of(s.split(" ")[0], s.split(" ")[1]);
                    }
                });

        DataStream<Tuple2<String, String>> input2 = env.socketTextStream("192.168.217.110", 9001)
                .map(new MapFunction<String, Tuple2<String, String>>() {

                    @Override
                    public Tuple2<String, String> map(String s) throws Exception {
                        return Tuple2.of(s.split(" ")[0], s.split(" ")[1]);
                    }
                });

        input1.coGroup(input2)
                .where(new KeySelector<Tuple2<String, String>, Object>() {

                    @Override
                    public Object getKey(Tuple2<String, String> value) throws Exception {
                        return value.f0;
                    }
                }).equalTo(new KeySelector<Tuple2<String, String>, Object>() {

            @Override
            public Object getKey(Tuple2<String, String> value) throws Exception {
                return value.f0;
            }
        }).window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
                .trigger(CountTrigger.of(1))
                .apply(new CoGroupFunction<Tuple2<String, String>, Tuple2<String, String>, Object>() {

                    @Override
                    public void coGroup(Iterable<Tuple2<String, String>> iterable, Iterable<Tuple2<String, String>> iterable1, Collector<Object> collector) throws Exception {
                        StringBuffer buffer = new StringBuffer();
                        buffer.append("DataStream frist:\n");
                        for (Tuple2<String, String> value : iterable) {
                            buffer.append(value.f0 + "=>" + value.f1 + "\n");
                        }
                        buffer.append("DataStream second:\n");
                        for (Tuple2<String, String> value : iterable1) {
                            buffer.append(value.f0 + "=>" + value.f1 + "\n");
                        }
                        collector.collect(buffer.toString());
                    }
                }).print();

        //输出：
        //2> DataStream frist:
        //2=>ac
        //DataStream second:
        //
        //4> DataStream frist:
        //DataStream second:
        //1=>lj
        //
        //4> DataStream frist:
        //1=>ao
        //DataStream second:
        //
        //4> DataStream frist:
        //DataStream second:
        //1=>al
        //
        //2> DataStream frist:
        //2=>14
        //DataStream second:
        //
        //2> DataStream frist:
        //2=>14
        //DataStream second:
        //2=>af

        env.execute();
    }
}
