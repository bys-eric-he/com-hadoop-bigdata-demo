package com.hadoop.web.mapping;

import com.hadoop.web.entity.UserRetentionDayRate;
import com.hadoop.web.model.UserRetentionDayRateModel;

/**
 * 每日用户留存情况统计表
 */
public class UserRetentionDayRateMapping {
    /**
     * Entity转换Model
     *
     * @param userRetentionDayRate
     * @return
     */
    public static UserRetentionDayRateModel toModel(UserRetentionDayRate userRetentionDayRate) {
        if (userRetentionDayRate == null) {
            return null;
        }

        UserRetentionDayRateModel result = new UserRetentionDayRateModel();
        result.setCreateDate(userRetentionDayRate.getDate().getCreateDate());
        result.setDateTime(userRetentionDayRate.getDate().getDateTime());
        result.setNewMidCount(userRetentionDayRate.getNewMidCount());
        result.setRetentionCount(userRetentionDayRate.getRetentionCount());
        result.setRetentionDay(userRetentionDayRate.getRetentionDay());
        result.setRetentionRatio(userRetentionDayRate.getRetentionRatio());

        return result;
    }
}
