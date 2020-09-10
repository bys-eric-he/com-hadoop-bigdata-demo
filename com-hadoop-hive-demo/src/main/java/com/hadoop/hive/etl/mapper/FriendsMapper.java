package com.hadoop.hive.etl.mapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * 查找共同的好友
 */
@Slf4j
@Component
public class FriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();

    /**
     * 读取 friends.txt 内容格式 A:B,C,D,F,E,O
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        // 根据冒号拆分
        String[] personFriends = line.split(":");
        // 第一个为用户
        String person = personFriends[0];
        // 第二个为好友
        String friends = personFriends[1];
        // 好友根据逗号拆分
        String[] friendsList = friends.split(",");
        log.info("----->好友数据信息,用户->{},好友->{}", person, friends);
        for (String friend : friendsList) {
            k.set(friend);
            v.set(person);
            context.write(k, v);
        }
    }
}
