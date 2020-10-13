package com.hadoop.web.entity;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Data
@Embeddable
public class DateTimePrimaryKey implements Serializable {
    @Column(name = "dt", nullable = false)
    private String dateTime;
}
