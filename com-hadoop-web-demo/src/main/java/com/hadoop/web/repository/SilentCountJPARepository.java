package com.hadoop.web.repository;

import com.hadoop.web.entity.SilentCount;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * 沉默用户数统计表
 */
@Repository
public interface SilentCountJPARepository extends JpaRepository<SilentCount, String> {
}

