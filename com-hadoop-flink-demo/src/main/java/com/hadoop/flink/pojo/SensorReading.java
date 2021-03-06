package com.hadoop.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SensorReading {
    /**
     * 传感器id
     */
    private String id;
    /**
     * 时间戳
     */
    private Long timestamp;
    /**
     * 温度值
     */
    private Double temperature;

}
