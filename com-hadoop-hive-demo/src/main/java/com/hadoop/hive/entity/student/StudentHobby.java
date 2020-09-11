package com.hadoop.hive.entity.student;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class StudentHobby {
    private int id;
    private String name;
    private List<String> hobby;
    private String add;
}
