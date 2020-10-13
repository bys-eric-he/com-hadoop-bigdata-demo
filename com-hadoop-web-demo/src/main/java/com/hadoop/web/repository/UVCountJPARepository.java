package com.hadoop.web.repository;

import com.hadoop.web.entity.UVCount;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * 活跃设备数统计表
 */
@Repository
public interface UVCountJPARepository extends JpaRepository<UVCount, String> {
}
