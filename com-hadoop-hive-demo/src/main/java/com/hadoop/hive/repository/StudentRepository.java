package com.hadoop.hive.repository;

import com.hadoop.hive.annotation.LogAspect;
import com.hadoop.hive.entity.student.Student;
import com.hadoop.hive.entity.student.StudentHobby;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.stereotype.Repository;

import java.util.List;

@Slf4j
@Repository
public class StudentRepository extends HiveBaseJDBCTemplate {
    /**
     * 获取hive数据库数据信息
     * SQL指令必须要有列，不可以select * from .....，否则BeanPropertyRowMapper无法映射到类对象中。
     *
     * @param sql HiveQL select id,name,score,age from student
     * @return
     */
    @LogAspect(value = "getLimitOne")
    public Student getLimitOne(String sql) {
        log.info("HiveQL-->{}", sql);
        //jdbcTemplate.queryForObject(sql, requiredType) 中的requiredType应该为基础类型，和String类型.
        //return this.getJdbcTemplate().queryForObject(sql, Student.class);

        //如果想查真正的object应该为
        List<Student> studentList = this.getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(Student.class));
        if (studentList.size() > 0) {
            return studentList.get(0);
        }
        return null;
    }

    /**
     * 获取数据列表
     *
     * @param sql SQL指令必须要有列，且列名必须和对象中的属性保持一致，不可以select * from .....，否则BeanPropertyRowMapper无法映射到类对象中。
     * @return
     */
    @LogAspect(value = "getListForObject")
    public List<Student> getListForObject(String sql) {
        //jdbcTemplate.queryForList(sql, requiredType) 中的requiredType应该为基础类型，和String类型.
        //return this.getJdbcTemplate().queryForList(sql, Student.class);

        //如果想查真正的object应该为
        return this.getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(Student.class));
    }

    /**
     * 获取学生爱好列表
     *
     * @param sql
     * @return
     */
    @LogAspect(value = "getListStudentHobby")
    public List<StudentHobby> getListStudentHobby(String sql) {
        return this.getJdbcTemplate().query(sql, new BeanPropertyRowMapper<>(StudentHobby.class));
    }
}
