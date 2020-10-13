package com.hadoop.web.entity;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Data
@Embeddable
public class CreateDatePrimaryKey implements Serializable {
    @Column(name = "create_date", nullable = false)
    private String createDate;
}
