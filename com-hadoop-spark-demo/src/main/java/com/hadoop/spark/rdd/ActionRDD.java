package com.hadoop.spark.rdd;

import com.hadoop.spark.common.SpringContextHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.beans.Transient;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * RDD支持两种操作：转化操作和行动操作。
 * RDD的转化操作是返回一个新的RDD的操作，比如map()和filter()。
 * 而行动操作则是想驱动器程序返回结果或把结果写入外部系统的操作，会触发实际的计算，比如count()和first()。
 */
@Slf4j
@Component
public class ActionRDD implements Serializable {

    private static final String INPUT_TXT_PATH
            = ActionRDD.class.getClassLoader().getResource("spark_data_demo.txt").toString();

    private static ThreadLocal<JavaSparkContext> threadLocal = new ThreadLocal<JavaSparkContext>() {
        protected JavaSparkContext initialValue() {
            return SpringContextHolder.getBean(JavaSparkContext.class);
        }
    };

    /**
     * 聚合（整合数据）
     */
    public void reduce() {
        JavaRDD<Integer> parallelize = threadLocal.get().parallelize(Arrays.asList(1, 2, 3, 4), 3);
        Tuple2<Integer, Integer> reduce = parallelize.mapToPair(x -> new Tuple2<>(x, 1))
                .reduce((x, y) -> getReduce(x, y));
        log.info("-->数组sum:" + reduce._1 + " -->计算次数:" + (reduce._2 - 1));
    }

    /**
     * 计算逻辑
     * （x）总和->数组的每一个数相加总和
     * （y）总和 ->计算次数
     *
     * @param x the x
     * @param y the y
     * @return the reduce
     */
    @Transient
    public Tuple2 getReduce(Tuple2<Integer, Integer> x, Tuple2<Integer, Integer> y) {
        Integer a = x._1();
        Integer b = x._2();
        a += y._1();
        b += y._2();
        return new Tuple2(a, b);
    }

    /**
     * 收集所有元素
     */
    public void collect() {
        JavaRDD<String> stringJavaRDD = threadLocal.get().textFile(INPUT_TXT_PATH);
        List<String> collect = stringJavaRDD.collect();
        checkResult(collect);
    }

    /**
     * 集合里面元素数量
     */
    public void count() {
        JavaRDD<String> stringJavaRDD = threadLocal.get().textFile(INPUT_TXT_PATH);
        long count = stringJavaRDD.count();
        log.info("-->集合里面元素数量:{}", count);
    }

    /**
     * 取第一个元素
     */
    public void first() {
        JavaRDD<String> stringJavaRDD = threadLocal.get().textFile(INPUT_TXT_PATH);
        String first = stringJavaRDD.first();
        log.info("-->第一个元素:{}", first);
    }

    /**
     * 取前N个数
     *
     * @param num N的值
     */
    public void take(int num) {
        JavaRDD<String> stringJavaRDD = threadLocal.get().textFile(INPUT_TXT_PATH);
        List<String> take = stringJavaRDD.take(num);
        log.info("-->前{}个数:{}", num, take);

    }

    /**
     * 取Key对应元素数量
     */
    public void countByKey() {
        JavaRDD<String> stringJavaRDD = threadLocal.get().textFile(INPUT_TXT_PATH);
        Map<String, Long> stringLongMap = stringJavaRDD.map(x -> x.split(",")[0])
                .mapToPair(x -> new Tuple2<>(x, 1))
                .countByKey();

        for (String key : stringLongMap.keySet()) {
            log.info("-->Key对应元素数量：{}", key + " + " + stringLongMap.get(key));
        }
    }

    /**
     * 循环
     */
    public void forEach() {
        JavaRDD<String> stringJavaRDD = threadLocal.get().textFile(INPUT_TXT_PATH);
        stringJavaRDD.foreach(x -> {
            log.info("-->循环打印:{}", x);
        });
    }

    /**
     * 打印测试
     *
     * @param collect the collect
     */
    private void checkResult(List<?> collect) {
        for (Object o : collect) {
            log.info("-->元素：{}", o.toString());
        }
    }
}
