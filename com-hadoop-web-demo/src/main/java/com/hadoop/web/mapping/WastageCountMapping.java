package com.hadoop.web.mapping;

import com.hadoop.web.entity.WastageCount;
import com.hadoop.web.model.WastageCountModel;

/**
 * 流失用户数统计表
 */
public class WastageCountMapping {
    /**
     * Entity转换Model
     *
     * @param wastageCount
     * @return
     */
    public static WastageCountModel toModel(WastageCount wastageCount) {
        if (wastageCount == null) {
            return null;
        }
        WastageCountModel result = new WastageCountModel();
        result.setDateTime(wastageCount.getDateTime().getDateTime());
        result.setWastageCount(wastageCount.getWastageCount());

        return result;
    }
}
