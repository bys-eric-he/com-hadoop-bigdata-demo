package com.hadoop.web.mapping;

import com.hadoop.web.entity.BackCount;
import com.hadoop.web.model.BackCountModel;

/**
 * 本周回流用户数统计表
 */
public class BackCountMapping {
    /**
     * Entity转换Model
     *
     * @param backCount
     * @return
     */
    public static BackCountModel toModel(BackCount backCount) {
        if (backCount == null) {
            return null;
        }

        BackCountModel result = new BackCountModel();
        result.setDateTime(backCount.getDateTime().getDateTime());
        result.setWastageCount(backCount.getWastageCount());
        return result;
    }
}
