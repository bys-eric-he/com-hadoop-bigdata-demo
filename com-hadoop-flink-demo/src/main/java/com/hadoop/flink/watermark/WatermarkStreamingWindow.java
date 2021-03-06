package com.hadoop.flink.watermark;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;

/**
 * Window和Watermark结合处理数据乱序
 * 这里面有两个时间概念：生成watermark的时间，允许乱序的时间和allowedLatest
 */
public class WatermarkStreamingWindow {
    public static void main(String[] args) throws Exception {

        //2021-03-04 19:30:31.000
        Long baseTimestamp = 1614857430000L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //将eventTime设置为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // AssignerWithPeriodicWatermarks子类是每隔一段时间执行的，这个具体由ExecutionConfig.setAutoWatermarkInterval设置，
        // 如果没有设置会一直调用getCurrentWatermark方法，之所以会出现-10000时因为你没有数据进入窗口，当然一直都是-10000，
        // 但是getCurrentWatermark方法不是在执行extractTimestamp后才执行的。所以，每条记录打印出的watermark，都应该是上一条的watermark
        env.getConfig().setAutoWatermarkInterval(2000);
        env.setParallelism(1);

        // 用parameter tool工具从程序启动参数中提取配置项
        //定义socket的端口号
        String host = null;
        int port = 8080;

        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            host = parameterTool.get("host");
            port = parameterTool.getInt("port");

        } catch (Exception e) {
            System.err.println("-->没有指定host和port参数!");
        }

        System.out.println("-->host:" + host + " -->port:" + port);

        DataStream<Tuple2<String, Long>> raw = env.socketTextStream(host, port, "\n").map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                //每行输入数据形如: key1@0,key1@13等等，即在baseTimestamp的基础上加多少秒，作为当前event time
                String[] tmp = value.split("@");
                //将当前时间加指定秒数
                Long ts = baseTimestamp + Long.parseLong(tmp[1]) * 1000;
                return Tuple2.of(tmp[0], ts);
            }
        }).filter(new FilterFunction<Tuple2<String, Long>>() {
            @Override
            public boolean filter(Tuple2<String, Long> stringLongTuple2) throws Exception {
                //过虑掉key1中值为0的数据
                return stringLongTuple2.f0.equals("key1") && stringLongTuple2.f1 != 0;
            }
        })//允许10秒乱序，watermark为当前接收到的最大事件时间戳减10秒
                .assignTimestampsAndWatermarks(new MyTimestampExtractor(Time.seconds(10)));

        //迟到的数据
        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("late") {
        };

        //全窗口函数
        SingleOutputStreamOperator<String> window = raw.keyBy(0)
                //窗口都为自然时间窗口，而不是说从收到的消息时间为窗口开始时间来进行开窗，比如3秒的窗口，那么窗口一次是[0,3),[3,6)....[57,0),如果10秒窗口，那么[0,10),[10,20),...
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                // 允许5秒延迟
                // 比如窗口[2018-03-03 03:30:00,2018-03-03 03:30:03),如果没有允许延迟的话，
                // 那么当watermark到达2018-03-03 03:30:03的时候，将会触发窗口函数并移除窗口，这样2018-03-03 03:30:03之前的数据再来，将被丢弃
                // 在允许5秒延迟的情况下，那么窗口的移除时间将到watermark为2018-03-03 03:30:08,
                // 在watermark没有到达这个时间之前，你输入2018-03-03 03:30:00这个时间，将仍然会触发[2018-03-03 03:30:00,2018-03-03 03:30:03)这个窗口的计算
                .allowedLateness(Time.seconds(5))
                // 收集迟到的数据, sideOutputLateData() 是一个兜底方案，数据延迟严重，可以保证数据不丢失。延迟的数据通过outputTag输出，必须要事件时间大于watermark + allowed lateness，数据才会存储在outputTag中
                .sideOutputLateData(outputTag)
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                        LinkedList<Tuple2<String, Long>> data = new LinkedList<>();
                        for (Tuple2<String, Long> tuple2 : input) {
                            data.add(tuple2);
                        }
                        data.sort(new Comparator<Tuple2<String, Long>>() {
                            @Override
                            public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                                return o1.f1.compareTo(o2.f1);
                            }
                        });
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String msg = String.format("----->键值:%s,  窗口时间:[ %s  ,  %s ), 元素个数:%d, 元数时间区间:[ %s  ,  %s ]", tuple.getField(0)
                                , format.format(new Date(window.getStart()))
                                , format.format(new Date(window.getEnd()))
                                , data.size()
                                , format.format(new Date(data.getFirst().f1))
                                , format.format(new Date(data.getLast().f1))
                        );
                        out.collect(msg);

                    }
                });
        // 调用getSideOutput()方法获取DataStream时，必须调用windowed operation返回的SingleOutputStreamOperator对象才能获取到期望的延迟数据。
        window.getSideOutput(outputTag).print("--->迟到的数据");

        window.print("-------- window 的计算结果 --------");

        env.execute();
    }

    /**
     * 周期性的生成watermark，生成间隔可配置，根据数据的eventTime来更新watermark时间。
     */
    public static class MyTimestampExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {

        private static final long serialVersionUID = 1L;

        /**
         * 目前看到的最大时间戳
         */
        private long currentMaxTimestamp;

        /**
         * 上次发出的水印的时间戳
         */
        private long lastEmittedWatermark = Long.MIN_VALUE;

        /**
         * 在记录中看到的最大时间戳和要发出的水印之间的（固定）间隔
         */
        private final long maxOutOfOrderness;


        private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        public MyTimestampExtractor(Time maxOutOfOrderness) {
            if (maxOutOfOrderness.toMilliseconds() < 0) {
                throw new RuntimeException("------试图将允许的最大延迟设置为" + maxOutOfOrderness + ", 此参数不能为负数.");
            }

            this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
            this.currentMaxTimestamp = Long.MIN_VALUE + this.maxOutOfOrderness;
        }

        public long getMaxOutOfOrdernessInMillis() {
            return maxOutOfOrderness;
        }

        /**
         * 定义生成Watermark的逻辑,默认100ms被调用一次
         * 产生频率：默认是来一条数据下发一次watermark，
         * 但是可以调整setAutoWatermarkInterval参数设置下发watermark的时间间隔，性能会有一定的提升。
         *
         * @return
         */
        @Override
        public final Watermark getCurrentWatermark() {
            // this guarantees that the watermark never goes backwards.
            long potentialWM = currentMaxTimestamp - maxOutOfOrderness;
            if (potentialWM >= lastEmittedWatermark) {
                lastEmittedWatermark = potentialWM;
            }
            System.out.println(String.format("--------->当前时间戳:%s  , 上一次时间戳:%s", format.format(new Date(currentMaxTimestamp)), format.format(new Date(lastEmittedWatermark))));
            //生成watermark的时间
            return new Watermark(lastEmittedWatermark);
        }

        /**
         * 定义如何提取timestamp
         *
         * @param element
         * @param previousElementTimestamp
         * @return
         */
        @Override
        public final long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
            long timestamp = element.f1;
            if (timestamp > currentMaxTimestamp) {
                currentMaxTimestamp = timestamp;
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            System.err.println("-------------键:" + element.getField(0) + "值:" + element.getField(1) + ", 当前时间戳:[" + currentMaxTimestamp + "|" +
                    sdf.format(currentMaxTimestamp) + "],水印时间:[" + getCurrentWatermark().getTimestamp() + "|" + sdf.format(getCurrentWatermark().getTimestamp()) + "]");
            return timestamp;
        }
    }
}
