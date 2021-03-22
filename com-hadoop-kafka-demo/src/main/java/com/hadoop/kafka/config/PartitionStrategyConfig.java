package com.hadoop.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 自定义分区策略
 * 在发送一条消息时,我们可以指定这个 Key,那么 Producer 会根据 Key 和 partition 机制来判断,
 * 当前这条消息应该发送并存储到哪个 partition 中（这个就跟分片机制类似）。
 * 我们可以根据需要进行扩展 Producer 的 partition 机制（默认算法是 hash 取 %）。
 */

@Slf4j
public class PartitionStrategyConfig implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //获取当前 topic 有多少个分区（分区列表）
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int partitionNum = 0;

        //Key 是可以传空值的
        if (key == null) {
            partitionNum = new Random().nextInt(partitions.size());   //随机
        } else {
            //取 %
            partitionNum = Math.abs((key.hashCode()) % partitions.size());
        }
        log.warn("自定义分区策略---->key：" + key + ",value：" + value + ",partitionNum：" + partitionNum + ",partitionSize: " + partitions.size());
        //发送到指定分区
        return partitionNum;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
