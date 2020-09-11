package com.hadoop.hive.service;

import com.hadoop.hive.entity.student.Student;
import com.hadoop.hive.entity.student.StudentHobby;

import java.util.List;

public interface StudentService {
    /**
     * 获取学生对象
     *
     * @param id
     * @return
     */
    Student getLimitOne(String id);

    /**
     * 获取学生对象列表
     *
     * @return
     */
    List<Student> getListForObject();

    /**
     * 获取学生爱好列表
     *
     * @return
     */
    List<StudentHobby> getListStudentHobby();
}
