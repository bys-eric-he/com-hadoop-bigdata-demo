package com.hadoop.kafka.entity;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@NoArgsConstructor
@ToString
public class UserOrderEntity {
    private String orderID;
    private String userID;
    private String number;
    private Double price;
    private String productID;
}
