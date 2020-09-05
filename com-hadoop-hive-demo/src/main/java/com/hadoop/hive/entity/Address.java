package com.hadoop.hive.entity;

import lombok.Data;

@Data
public class Address {
    private String street;
    private String city;
    private String state;
    private int zip;
}
