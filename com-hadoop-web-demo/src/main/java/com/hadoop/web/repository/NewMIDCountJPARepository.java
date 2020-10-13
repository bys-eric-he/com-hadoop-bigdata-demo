package com.hadoop.web.repository;

import com.hadoop.web.entity.NewMIDCount;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * 每日新增设备信息数量统计表
 */
@Repository
public interface NewMIDCountJPARepository extends JpaRepository<NewMIDCount, String> {
}

