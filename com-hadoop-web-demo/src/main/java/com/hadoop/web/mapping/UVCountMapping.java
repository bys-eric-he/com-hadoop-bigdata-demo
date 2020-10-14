package com.hadoop.web.mapping;

import com.hadoop.web.entity.UVCount;
import com.hadoop.web.model.UVCountModel;

/**
 * 活跃设备数统计表
 */
public class UVCountMapping {
    /**
     * Entity转换Model
     *
     * @param uvCount
     * @return
     */
    public static UVCountModel toModel(UVCount uvCount) {
        if (uvCount == null) {
            return null;
        }

        UVCountModel result = new UVCountModel();
        result.setDateTime(uvCount.getDateTime().getDateTime());
        result.setDayCount(uvCount.getDayCount());
        result.setMnCount(uvCount.getMnCount());
        result.setWkCount(uvCount.getWkCount());
        result.setIsMonthend(uvCount.getIsMonthend());
        result.setIsWeekend(uvCount.getIsWeekend());

        return result;
    }
}
