package com.hadoop.web.repository;

import com.hadoop.web.entity.WastageCount;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * 流失用户数统计表
 */
@Repository
public interface WastageCountJPARepository extends JpaRepository<WastageCount, String> {
}
