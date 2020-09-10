package com.hadoop.hive.etl.reducer;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * 获取共同好友
 */
@Slf4j
@Component
public class FriendsReducer extends Reducer<Text, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();

    /**
     * 读取 FriendsMapper 输出，内容格式 B A
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        // 循环好友
        for (Text person : values) {
            sb.append(person).append(",");
        }

        k.set(key);
        v.set(sb.toString());
        context.write(k, v);
        log.info("---->共同好友数据信息,用户->{},同时存在->{} 好友列表中！", key.toString(), sb.toString());
    }
}
