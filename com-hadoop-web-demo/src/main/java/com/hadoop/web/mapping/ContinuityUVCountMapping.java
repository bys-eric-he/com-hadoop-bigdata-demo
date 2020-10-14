package com.hadoop.web.mapping;

import com.hadoop.web.entity.ContinuityUVCount;
import com.hadoop.web.model.ContinuityUVCountModel;

/**
 * 连续活跃设备数
 */
public class ContinuityUVCountMapping {
    /**
     * Entity转换Model
     *
     * @param continuityUVCount
     * @return
     */
    public static ContinuityUVCountModel toModel(ContinuityUVCount continuityUVCount) {
        if (continuityUVCount == null) {
            return null;
        }

        ContinuityUVCountModel result = new ContinuityUVCountModel();
        result.setDateTime(continuityUVCount.getDateTime().getDateTime());
        result.setWeekDateTime(continuityUVCount.getWeekDateTime());
        result.setContinuityCount(continuityUVCount.getContinuityCount());

        return result;
    }
}
