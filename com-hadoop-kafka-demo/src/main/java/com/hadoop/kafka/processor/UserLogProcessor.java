package com.hadoop.kafka.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * 具体业务处理
 */
@Slf4j
public class UserLogProcessor implements Processor<byte[], byte[]> {

    private ProcessorContext context;

    //初始化
    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.context.schedule(1000);
    }

    /**
     * 处理,实时处理,每条都调用这个
     *
     * @param key
     * @param value
     */
    @Override
    public void process(byte[] key, byte[] value) {
        String input = new String(value);
        log.info("---> 当前from topic的内容: {}",input);
        // 如果包含“>>>”则只保留该标记后面的内容
        if (input.contains(">>>")) {
            input = input.split(">>>")[1].trim();
        }
        log.info("---> 输出到下一个to topic的内容: {}",input);
        // 输出到下一个topic
        context.forward("userLogProcessor".getBytes(), input.getBytes());
    }

    @Override
    public void punctuate(long l) {

    }

    //释放资源
    @Override
    public void close() {

    }
}