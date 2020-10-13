package com.hadoop.web.repository;

import com.hadoop.web.entity.UserRetentionDayRate;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * 每日用户留存情况统计表
 */
@Repository
public interface UserRetentionDayRateJPARepository extends JpaRepository<UserRetentionDayRate, String> {
}
