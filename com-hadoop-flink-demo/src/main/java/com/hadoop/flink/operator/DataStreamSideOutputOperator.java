package com.hadoop.flink.operator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SideOutPut: SideOutPut是Flink框架提供的最新的也是最为推荐的分流方法
 */
public class DataStreamSideOutputOperator {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境配置信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.定义数据源
        List data = new ArrayList<Tuple2<String, Integer>>();
        data.add(new Tuple2<>("666", 33));
        data.add(new Tuple2<>("Vidal.Deng", 27));
        data.add(new Tuple2<>("Alex.Zheng", 31));
        data.add(new Tuple2<>("Sky.Zhang", 34));
        data.add(new Tuple2<>("Felix.Huang", 33));
        data.add(new Tuple2<>("Robert.Luo", 33));

        final DataStreamSource<Tuple2<String, Integer>> streamSource = env.fromCollection(data);

        //3.定义OutPutTag
        final OutputTag<Tuple2<String, Integer>> strSplitStream = new OutputTag<Tuple2<String, Integer>>("strSplitStream") {
        };
        final OutputTag<Tuple2<String, Integer>> intSplitStream = new OutputTag<Tuple2<String, Integer>>("intSplitStream") {
        };


        final SingleOutputStreamOperator<Tuple2<String, Integer>> process = streamSource.process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                if (isNumeric(value.f0)) {
                    ctx.output(intSplitStream, value);
                } else {
                    ctx.output(strSplitStream, value);
                }
            }
        });

        final DataStream<Tuple2<String, Integer>> strSideOutput = process.getSideOutput(strSplitStream);
        final DataStream<Tuple2<String, Integer>> intSideOutput = process.getSideOutput(intSplitStream);

        //输出：
        //Name:4> (Vidal.Deng,27)
        //Name:5> (Alex.Zheng,31)
        //Name:7> (Felix.Huang,33)
        //Name:6> (Sky.Zhang,34)
        //Name:8> (Robert.Luo,33)
        strSideOutput.print("Name");
        //输出：Age:3> (666,33)
        intSideOutput.printToErr("Age");

        //4.二次分流
        final OutputTag<Tuple2<String, Integer>> secondSplitStream = new OutputTag<Tuple2<String, Integer>>("intSplitStream") {
        };

        final SingleOutputStreamOperator<Tuple2<String, Integer>> processSecond = strSideOutput.process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                if (value.f0.equals("Vidal.Deng")) {
                    ctx.output(secondSplitStream, value);
                }
            }
        });

        final DataStream<Tuple2<String, Integer>> strSecondSideOutput = processSecond.getSideOutput(secondSplitStream);

        // 输出: 邓嘉腾:4> (Vidal.Deng,27)
        strSecondSideOutput.printToErr("邓嘉腾");

        String jobName = "用户自定义数据源";
        //5.开始执行
        env.execute(jobName);
    }

    private static boolean isNumeric(String str) {
        Pattern pattern = Pattern.compile("[0-9]*");
        Matcher isNum = pattern.matcher(str);
        return isNum.matches();
    }
}
