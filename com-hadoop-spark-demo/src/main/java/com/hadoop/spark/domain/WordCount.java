package com.hadoop.spark.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

@Data
@ToString
@AllArgsConstructor
public class WordCount implements Serializable {
    private String word;
    private Integer count;
}
