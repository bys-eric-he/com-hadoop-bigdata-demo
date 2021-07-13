package com.hadoop.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CountWithTimestamp {
    public String key;
    public long count;
    public long lastModified;
}
