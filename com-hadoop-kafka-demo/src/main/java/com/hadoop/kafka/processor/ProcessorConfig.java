package com.hadoop.kafka.processor;

import com.hadoop.kafka.common.KafkaUtils;
import com.hadoop.kafka.common.TopicConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class ProcessorConfig {
    /**
     * 指定kafka server的地址，集群可配多个，中间，逗号隔开
     */
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServersConfig;

    @Autowired
    private ApplicationContext context;

    @PostConstruct
    public void init() {
        KafkaUtils kafkaUtils = context.getBean(KafkaUtils.class);
        //创建二个新的主题
        kafkaUtils.createSingleTopic(TopicConstant.USER_LOG_PROCESSOR_TOPIC_FROM, 2, (short) 1);
        kafkaUtils.createSingleTopic(TopicConstant.USER_LOG_PROCESSOR_TOPIC_TO, 2, (short) 1);

        //创建两个主题streams-plaintext-input与streams-wordcount-output
        kafkaUtils.createSingleTopic(TopicConstant.KAFKA_STREAMS_PIPE_OUTPUT, 2, (short) 1);
        kafkaUtils.createSingleTopic(TopicConstant.KAFKA_STREAMS_LINESPLIT_OUTPUT, 2, (short) 1);

        try {
            TimeUnit.SECONDS.sleep(1);

            List<String> queryAllTopic = kafkaUtils.queryAllTopic();
            log.info("---->当前系统所有消息主题：{}", queryAllTopic.toString());

            /*
            以下方法会引发异常
            this.userLogConfig();
            this.supplerConfig();

            this.pipeStream();
            this.lineSplit();*/
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * UserLogProcessor配置
     * 日志生产者发送日志数据到TopicConstant.USER_LOG_PROCESSOR_TOPIC_FROM
     */
    public void userLogConfig() {
        // 定义输入的topic
        String from = TopicConstant.USER_LOG_PROCESSOR_TOPIC_FROM;
        // 定义输出的topic
        String to = TopicConstant.USER_LOG_PROCESSOR_TOPIC_TO;

        // 设置参数
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka_consumer_group_demo");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        //指定分区策略
        settings.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.hadoop.kafka.config.PartitionStrategyConfig");


        StreamsConfig config = new StreamsConfig(settings);

        // 构建拓扑
        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("SOURCE", from);
        //UserLogProcessor 对Topic TopicConstant.USER_LOG_PROCESSOR_TOPIC_FROM 中数据进行清洗
        builder.addProcessor("PROCESSOR-01", new ProcessorSupplier<byte[], byte[]>() {
            @Override
            public Processor<byte[], byte[]> get() {
                // 具体分析处理
                return new UserLogProcessor();
            }
        }, "SOURCE");

        // 把处理后的数据保存到Topic TopicConstant.USER_LOG_PROCESSOR_TOPIC_TO
        // 消费者消费 TopicConstant.USER_LOG_PROCESSOR_TOPIC_TO 中的数据
        builder.addSink("SINK", to, "PROCESSOR-01");

        // 创建kafka stream
        KafkaStreams streams = new KafkaStreams(builder, config);

        try {
            streams.start();
            log.info("-------用户日志流式处理启动----------");
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    /**
     * WordCountProcessorSupplier配置
     */
    public void supplerConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka_consumer_group_demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE", "wc-input");
        builder.addProcessor("PROCESSOR-02", new WordCountProcessorSupplier(), "SOURCE");
        builder.addStateStore(Stores.create("Counts").withStringKeys().withIntegerValues().inMemory().build(),
                "PROCESSOR-02");
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

    /**
     * 实时流计算
     * 调用该方法，并在kafka服务中通过命令行脚本订阅kafka_streams_pipe_output主题
     * ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafka_streams_pipe_output  --from-beginning
     */
    public void pipeStream() {
        // 设置参数
        Properties settings = new Properties();
        //GroupID
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka_consumer_streams_pipe");
        //连接地址
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        //键的反序列化方式
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //值的反序列化方式
        settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(TopicConstant.KAFKA_STREAMS_PIPE_INPUT).to(TopicConstant.KAFKA_STREAMS_PIPE_OUTPUT);

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, settings);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
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
            System.exit(1);
        }
        System.exit(0);
    }

    /**
     * 实时流计算,将实时输入的文本内容按空格拆分
     * 调用该方法，并在kafka服务中通过命令行脚本订阅kafka_streams_linesplit_output主题
     * ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafka_streams_linesplit_output  --from-beginning
     */
    public void lineSplit() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka_consumer_streams-linesplit");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream(TopicConstant.KAFKA_STREAMS_PIPE_INPUT);
        source.flatMapValues(value -> Arrays.asList(value.split("\\W+"))).to(TopicConstant.KAFKA_STREAMS_LINESPLIT_OUTPUT);

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
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
            System.exit(1);
        }
        System.exit(0);
    }
}
