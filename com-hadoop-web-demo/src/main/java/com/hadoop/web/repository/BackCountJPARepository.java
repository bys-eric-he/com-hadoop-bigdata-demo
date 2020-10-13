package com.hadoop.web.repository;

import com.hadoop.web.entity.BackCount;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * 本周回流用户数统计
 */
@Repository
public interface BackCountJPARepository extends JpaRepository<BackCount, String> {
}
