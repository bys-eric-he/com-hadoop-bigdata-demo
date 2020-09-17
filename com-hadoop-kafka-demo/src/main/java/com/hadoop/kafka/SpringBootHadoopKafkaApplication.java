package com.hadoop.kafka;

import com.hadoop.kafka.processor.UserLogProcessor;
import com.hadoop.kafka.processor.WordCountProcessorSupplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@SpringBootApplication
public class SpringBootHadoopKafkaApplication {
    public static void main(String[] args) {
        SpringApplication.run(SpringBootHadoopKafkaApplication.class, args);
    }

    /**
     * UserLogProcessor配置
     */
    public static void userLogConfig() {
        // 定义输入的topic
        String from = "first";
        // 定义输出的topic
        String to = "second";

        // 设置参数
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "log-filter-demo");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

        StreamsConfig config = new StreamsConfig(settings);

        // 构建拓扑
        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("SOURCE", from)
                .addProcessor("PROCESS", new ProcessorSupplier<byte[], byte[]>() {

                    @Override
                    public Processor<byte[], byte[]> get() {
                        // 具体分析处理
                        return new UserLogProcessor();
                    }
                }, "SOURCE")
                .addSink("SINK", to, "PROCESS");

        // 创建kafka stream
        KafkaStreams streams = new KafkaStreams(builder, config);
        try {
            streams.start();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    /**
     * WordCountProcessorSupplier配置
     */
    public static void supplerConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE", "wc-input");
        builder.addProcessor("PROCESSOR1", new WordCountProcessorSupplier(), "SOURCE");
        builder.addStateStore(Stores.create("Counts").withStringKeys().withIntegerValues().inMemory().build(),
                "PROCESSOR1");

        // 创建kafka stream
        KafkaStreams streams = new KafkaStreams(builder, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
