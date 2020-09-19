package com.hadoop.kafka.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class UserOrder implements Serializable {
    private static final long serialVersionUID = 5071239632319759223L;
    private String orderID;
    private String userID;
    private String number;
    private Double price;
    private String productID;
}
