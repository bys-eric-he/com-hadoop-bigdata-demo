package com.hadoop.hive.repository;

import com.hadoop.hive.annotation.LogAspect;
import com.hadoop.hive.entity.user.UserAccessTimes;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Slf4j
@Repository
public class UserAccessTimesRepository extends HiveBaseJDBCTemplate {
    /**
     * 获取数据列表
     *
     * @param sql SQL指令必须要有列，且列名必须和对象中的属性保持一致，不可以select * from .....，否则BeanPropertyRowMapper无法映射到类对象中。
     * @return
     */
    @LogAspect(value = "getUserAccessTimesList")
    public List<UserAccessTimes> getUserAccessTimesList(String sql) {
        return this.getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(UserAccessTimes.class));
    }
}
