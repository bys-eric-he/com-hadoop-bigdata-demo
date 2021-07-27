package com.hadoop.flink.processfunction;

import com.hadoop.flink.pojo.SensorReading;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class DataStreamKeyedProcessFunction {
    /**
     * 主函数入口
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream(host, port);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 测试KeyedProcessFunction，先分组然后自定义处理
        KeyedStream<SensorReading, String> result = dataStream.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading value) throws Exception {
                return value.getId();
            }
        });

        result.process(new MyProcess())
                .print("----KeyedProcessFunction 处理结果-----");

        env.execute();
    }

    /**
     * 实现自定义的处理函数
     */
    public static class MyProcess extends KeyedProcessFunction<String, SensorReading, Integer> {
        // 自定义状态，利用State进行统计温度
        // ValueState: 状态保存的是一个值，可以通过update()来更新，value()获取。
        // ListState: 状态保存的是一个列表，通过add()添加数据，通过get()方法返回一个Iterable来遍历状态值。
        // ReducingState: 这种状态通过用户传入的reduceFunction，每次调用add方法添加值的时候，会调用reduceFunction，最后合并到一个单一的状态值。
        // MapState：即状态值为一个map。用户通过put或putAll方法添加元素。
        ValueState<Double> tsTimerState;

        /**
         * 初始化状态
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("ts-timer", Double.class));
        }

        /**
         * 每有一个数据进入算子，则会触发一次processElement()的处理
         * processElement的参数，依次是【输入值】、【上下文】、【输出值】
         *
         * @param value
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());

            ctx.timestamp();
            ctx.getCurrentKey();
            ctx.timerService().currentProcessingTime();
            ctx.timerService().currentWatermark();
            //注册事件时间定时器
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
            tsTimerState.update(value.getTemperature());
            /*
            ctx.timerService().registerEventTimeTimer((value.getTimestamp() + 10) * 1000L);
            ctx.timerService().deleteProcessingTimeTimer(tsTimerState.value());
            */
        }

        /**
         * 定时器触发后执行的方法, 在定时器满足时间条件时，会触发onTimer，可以用out输出返回值。
         *
         * @param timestamp 这个时间戳代表的是该定时器的触发时间
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {

            // 取得当前值
            String currentKey = ctx.getCurrentKey();
            // 取得State状态中的温度值
            Double temperature = tsTimerState.value();

            System.out.println("onTimer被触发,触发时间：" + timestamp + " 传感器id:" + currentKey + " 当前温度：" + temperature);
            ctx.timeDomain();
        }

        @Override
        public void close() throws Exception {
            tsTimerState.clear();
        }
    }
}
