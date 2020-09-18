package com.hadoop.kafka.model;

import lombok.Data;

@Data
public class UserOrder {
    private String orderID;
    private String userID;
    private String number;
    private Double price;
    private String productID;
}
