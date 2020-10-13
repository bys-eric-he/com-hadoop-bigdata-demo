package com.hadoop.web.repository;

import com.hadoop.web.entity.ContinuityWKCount;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * 最近连续三周活跃用户数统计
 */
@Repository
public interface ContinuityWKCountJPARepository extends JpaRepository<ContinuityWKCount, String> {
}
