package com.hadoop.hive.service.ods.impl;

import com.hadoop.hive.common.DateUtil;
import com.hadoop.hive.entity.log.EventLog;
import org.springframework.stereotype.Service;

@Service
public class EventLogODSServiceImpl extends AbstractODSService<EventLog> {

    @Override
    protected String takeContext(EventLog startLog) {
        return String.format("INSERT OVERWRITE TABLE ods_event_log PARTITION (dt = '%s')" +
                        "SELECT '%s'",
                DateUtil.getLocalDateTime(startLog.getDateTime(), DateUtil.DEFAULT_DATE_FORMAT),
                DateUtil.getLocalDateTime(startLog.getDateTime(), DateUtil.DEFAULT_DATE_TIME_FORMAT) + "%" + startLog.getContext());
    }
}
