package com.hadoop.flink.processfunction;

import com.hadoop.flink.operator.SplitterFlatMapOperator;
import com.hadoop.flink.pojo.CountWithTimestamp;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class CountWithTimeoutKeyedProcessFunction {
    /**
     * KeyedProcessFunction的子类，作用是将每个单词最新出现时间记录到backend，并创建定时器，
     * 定时器触发的时候，检查这个单词距离上次出现是否已经达到10秒，如果是，就发射给下游算子
     */
    static class CountWithTimeoutFunction extends KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Long>> {

        // 自定义状态，利用State进行统计
        private ValueState<CountWithTimestamp> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化状态，name是myState
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
        }

        /**
         * 每有一个数据进入算子，则会触发一次processElement()的处理
         * processElement的参数，依次是【输入值】、【上下文】、【输出值】
         *
         * @param value
         * @param ctx   上下文
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {

            // 获取当前流的key,取得当前是哪个单词
            Tuple currentKey = ctx.getCurrentKey();

            // 从backend取得当前单词的myState状态
            CountWithTimestamp current = state.value();

            // 如果myState还从未没有赋值过，就在此初始化
            if (current == null) {
                current = new CountWithTimestamp();
                current.key = value.f0;
            }

            // 单词数量加一
            current.count++;

            // 取当前元素的时间戳，作为该单词最后一次出现的时间
            current.lastModified = ctx.timestamp();

            // 重新保存到backend，包括该单词出现的次数，以及最后一次出现的时间
            state.update(current);

            // 为当前单词创建定时器，十秒后后触发
            long timer = current.lastModified + 10000;

            //注册事件时间定时器
            ctx.timerService().registerProcessingTimeTimer(timer);

            // 打印所有信息，用于核对数据正确性
            System.out.println(String.format("process, %s, %d, lastModified : %d (%s), timer : %d (%s)\n\n",
                    currentKey.getField(0),
                    current.count,
                    current.lastModified,
                    time(current.lastModified),
                    timer,
                    time(timer)));

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
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {

            // 取得当前单词
            Tuple currentKey = ctx.getCurrentKey();

            // 取得该单词的myState状态
            CountWithTimestamp result = state.value();

            // 当前元素是否已经连续10秒未出现的标志
            boolean isTimeout = false;

            // timestamp是定时器触发时间，如果等于最后一次更新时间+10秒，就表示这十秒内已经收到过该单词了，
            // 这种连续十秒没有出现的元素，被发送到下游算子
            if (timestamp == result.lastModified + 10000) {
                // 发送
                out.collect(new Tuple2<String, Long>(result.key, result.count));
                isTimeout = true;
            }

            // 打印数据，用于核对是否符合预期
            System.out.println(String.format("-->onTimer, %s, %d, -->lastModified : %d (%s), -->stamp : %d (%s), -->isTimeout : %s\n\n",
                    currentKey.getField(0),
                    result.count,
                    result.lastModified,
                    time(result.lastModified),
                    timestamp,
                    time(timestamp),
                    isTimeout));
        }

        /**
         * 清空当前状态值
         *
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            state.clear();
        }
    }

    /**
     * 主函数入口
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 并行度1
        env.setParallelism(1);

        // 处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

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

        // 监听本地8082端口，读取字符串
        DataStream<String> socketDataStream = env.socketTextStream(host, port);

        // 所有输入的单词，如果超过10秒没有再次出现，都可以通过CountWithTimeoutFunction得到
        DataStream<Tuple2<String, Long>> timeOutWord = socketDataStream
                // 对收到的字符串用空格做分割，得到多个单词
                .flatMap(new SplitterFlatMapOperator())
                // 设置时间戳分配器，用当前时间作为时间戳
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Integer>>() {

                    @Override
                    public long extractTimestamp(Tuple2<String, Integer> element, long previousElementTimestamp) {
                        // 使用当前系统时间作为时间戳
                        return System.currentTimeMillis();
                    }

                    /**
                     * 获取当前数据流的水位线
                     * @return
                     */
                    @Override
                    public Watermark getCurrentWatermark() {
                        // 本例不需要watermark，返回null
                        return null;
                    }
                })
                // 将单词作为key分区
                .keyBy(0)
                // 按单词分区后的数据，交给自定义KeyedProcessFunction处理
                .process(new CountWithTimeoutFunction());

        // 所有输入的单词，如果超过10秒没有再次出现，就在此打印出来
        timeOutWord.print("---所有输入的单词，如果超过10秒没有再次出现，就在此打印出来!---");

        env.execute("ProcessFunction 代码演示 : KeyedProcessFunction");
    }

    public static String time(long timeStamp) {
        return new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(timeStamp));
    }
}
