package com.hadoop.web.repository;

import com.hadoop.web.entity.ContinuityUVCount;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * 连续活跃设备数
 */
@Repository
public interface ContinuityUVCountJPARepository extends JpaRepository<ContinuityUVCount, String> {
}
