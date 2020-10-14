package com.hadoop.web.mapping;

import com.hadoop.web.entity.SilentCount;
import com.hadoop.web.model.SilentCountModel;

/**
 * 沉默用户数统计表
 */
public class SilentCountMapping {
    /**
     * Entity转换Model
     *
     * @param silentCount
     * @return
     */
    public static SilentCountModel toModel(SilentCount silentCount) {
        if (silentCount == null) {
            return null;
        }

        SilentCountModel result = new SilentCountModel();
        result.setDateTime(silentCount.getDateTime().getDateTime());
        result.setSilentCount(silentCount.getSilentCount());

        return result;
    }
}
