package com.hadoop.web.mapping;

import com.hadoop.web.entity.ContinuityWKCount;
import com.hadoop.web.model.ContinuityWKCountModel;

/**
 * 最近连续三周活跃用户数统计
 */
public class ContinuityWKCountMapping {
    /**
     * Entity转换Model
     *
     * @param continuityWKCount
     * @return
     */
    public static ContinuityWKCountModel toModel(ContinuityWKCount continuityWKCount) {
        if (continuityWKCount == null) {
            return null;
        }

        ContinuityWKCountModel result = new ContinuityWKCountModel();
        result.setDateTime(continuityWKCount.getDateTime().getDateTime());
        result.setWeekDateTime(continuityWKCount.getWeekDateTime());
        result.setContinuityCount(continuityWKCount.getContinuityCount());

        return result;
    }
}
