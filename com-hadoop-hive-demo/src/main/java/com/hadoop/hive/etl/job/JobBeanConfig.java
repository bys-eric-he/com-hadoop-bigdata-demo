package com.hadoop.hive.etl.job;

import com.hadoop.hive.config.DataSourceProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 用于配置和注册带参数的Job实例到IOC中
 */
@Configuration
public class JobBeanConfig {

    @Autowired
    private DataSourceProperties dataSourceProperties;

    /**
     * 单词统计
     *
     * @return
     */
    @Bean("wordCountJob")
    public WordCountJob wordCountJob() {
        return new WordCountJob(
                WordCountJob.class.getName(),
                dataSourceProperties.getHdfs().get("url") + "/eric/hadoop_data/input/words.txt",
                dataSourceProperties.getHdfs().get("url") + "/eric/hadoop_data/output_words");
    }

    /**
     * 共同好友统计
     *
     * @return
     */
    @Bean("friendsJob")
    public FriendsJob friendsJob() {
        return new FriendsJob(
                FriendsJob.class.getName(),
                dataSourceProperties.getHdfs().get("url") + "/eric/hadoop_data/input/friends.txt",
                dataSourceProperties.getHdfs().get("url") + "/eric/hadoop_data/output_friends"
        );
    }

    /**
     * 统计最高温度
     *
     * @return
     */
    @Bean("weatherJob")
    public WeatherJob weatherJob() {
        return new WeatherJob(
                WeatherJob.class.getName(),
                dataSourceProperties.getHdfs().get("url") + "/eric/hadoop_data/input/weathers.txt",
                dataSourceProperties.getHdfs().get("url") + "/eric/hadoop_data/output_weathers"
        );
    }
}
