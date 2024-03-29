package com.hadoop.flink.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DataStreamRichFlatMapOperator {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //定义socket的端口号
        String host = null;
        int port = 8080;


        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            host = parameterTool.get("host");
            port = parameterTool.getInt("port");

            System.out.println("-->当前监听服务IP:" + host + "端口:" + port);
        } catch (Exception e) {
            System.err.println("-->没有指定host和port参数!");
            return;
        }

        DataStreamSource<String> localhost = executionEnvironment.socketTextStream(host, port);

        // 输入a,1这样的数据，计算连续两个相同key的数量差值
        // 差值不能大于10
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = localhost.map(
                new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new Tuple2<>(split[0], Integer.valueOf(split[1]));
                    }
                }
        );
        map
                .keyBy(0)
                .flatMap(new MyRichFlatMapFunction())
                .print("---报警数据类型---");

        executionEnvironment.execute();
    }

    /**
     * 参数1：是输入的数据类型
     * 参数2：是报警时显示的数据类型
     */
    public static class MyRichFlatMapFunction extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, String>> {

        class StateFalg {
            String name;
            long count;
        }

        // ValueState: 状态保存的是一个值，可以通过update()来更新，value()获取。
        // ListState: 状态保存的是一个列表，通过add()添加数据，通过get()方法返回一个Iterable来遍历状态值。
        // ReducingState: 这种状态通过用户传入的reduceFunction，每次调用add方法添加值的时候，会调用reduceFunction，最后合并到一个单一的状态值。
        // MapState：即状态值为一个map。用户通过put或putAll方法添加元素。
        ValueState<StateFalg> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 创建一个状态值
            // 通过创建一个StateDescriptor，可以得到一个包含特定名称的状态句柄
            // 可以分别创建ValueStateDescriptor、 ListStateDescriptor或ReducingStateDescriptor状态句柄。
            // 状态是通过RuntimeContext来访问的，因此只能在RichFunction中访问状态。
            // 这就要求UDF时要继承Rich函数，例如RichMapFunction、RichFlatMapFunction等。
            ValueStateDescriptor<StateFalg> valueStateDescriptor = new ValueStateDescriptor<>("state", StateFalg.class);
            state = getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public void flatMap(Tuple2<String, Integer> stringIntegerTuple2, Collector<Tuple2<String, String>> collector) throws Exception {
            // 判断状态值是否为空（状态默认值是空）
            if (state.value() == null) {
                StateFalg sFalg = new StateFalg();
                sFalg.name = stringIntegerTuple2.f0;
                sFalg.count = stringIntegerTuple2.f1;
                state.update(sFalg);
            }
            StateFalg value = state.value();
            if (Math.abs(value.count - stringIntegerTuple2.f1) > 10) {
                collector.collect(new Tuple2<String, String>("数量" + stringIntegerTuple2.f1 + "超出警戒值", stringIntegerTuple2.f0));
            }
            value.name = stringIntegerTuple2.f0;
            value.count = stringIntegerTuple2.f1;
            // 更新状态值
            state.update(value);
        }
    }

/*
    输入            输出
    a,1
    a,20           （数量超出警戒值,a） 　　　　　　
    b,1
    b,2
    c,1             (数量超出警戒值,a)
    a,1
    c,50            (数量超出警戒值,c)
**/
}
