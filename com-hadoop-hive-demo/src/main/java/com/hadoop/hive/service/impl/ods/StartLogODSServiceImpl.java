package com.hadoop.hive.service.impl.ods;

import com.hadoop.hive.common.DateUtil;
import com.hadoop.hive.entity.log.StartLog;
import org.springframework.stereotype.Service;

/**
 * 启动日志原始数据
 */
@Service
public class StartLogODSServiceImpl extends AbstractODSService<StartLog> {

    @Override
    protected String takeContext(StartLog startLog) {
        return String.format("INSERT OVERWRITE TABLE ods_start_log PARTITION (dt = '%s')" +
                "SELECT '%s'", DateUtil.getLocalDateTime(startLog.getDateTime(), DateUtil.DEFAULT_DATE_FORMAT), startLog.getContext());
    }
}
