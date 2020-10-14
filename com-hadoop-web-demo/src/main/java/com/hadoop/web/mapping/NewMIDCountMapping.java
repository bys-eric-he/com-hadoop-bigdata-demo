package com.hadoop.web.mapping;

import com.hadoop.web.entity.NewMIDCount;
import com.hadoop.web.model.NewMIDCountModel;

/**
 * 每日新增设备信息数量统计表
 */
public class NewMIDCountMapping {
    /**
     * Entity转换Model
     *
     * @param newMIDCount
     * @return
     */
    public static NewMIDCountModel toModel(NewMIDCount newMIDCount) {
        if (newMIDCount == null) {
            return null;
        }

        NewMIDCountModel result = new NewMIDCountModel();
        result.setCreateDate(newMIDCount.getCreateDate().getCreateDate());
        result.setNewMidCount(newMIDCount.getNewMidCount());
        return result;
    }
}
