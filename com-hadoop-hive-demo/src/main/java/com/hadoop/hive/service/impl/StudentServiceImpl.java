package com.hadoop.hive.service.impl;

import com.hadoop.hive.entity.student.Student;
import com.hadoop.hive.entity.student.StudentHobby;
import com.hadoop.hive.repository.StudentRepository;
import com.hadoop.hive.service.StudentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class StudentServiceImpl implements StudentService {

    @Autowired
    private StudentRepository studentRepository;

    /**
     * 获取学生对象
     *
     * @param id
     * @return
     */
    @Override
    public Student getLimitOne(String id) {
        String sql = "select id,name,score,age from student where id ='" + id + "'";
        return studentRepository.getLimitOne(sql);
    }

    /**
     * 获取学生对象列表
     *
     * @return
     */
    @Override
    public List<Student> getListForObject() {
        String sql = "select id,name,score,age from student";
        return studentRepository.getListForObject(sql);
    }

    /**
     * 获取学生爱好列表
     *
     * @return
     */
    @Override
    public List<StudentHobby> getListStudentHobby() {
        String sql = "select id,name,hobby,add from student";
        return studentRepository.getListStudentHobby(sql);
    }
}
