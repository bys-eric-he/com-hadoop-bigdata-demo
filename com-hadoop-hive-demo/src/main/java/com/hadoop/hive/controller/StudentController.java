package com.hadoop.hive.controller;

import com.hadoop.hive.common.Result;
import com.hadoop.hive.common.ResultUtil;
import com.hadoop.hive.entity.student.Student;
import com.hadoop.hive.entity.student.StudentHobby;
import com.hadoop.hive.service.StudentService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api(tags = "StudentController", description = "学生信息统计")
@RestController
@RequestMapping("/api/v1/student")
public class StudentController {
    @Autowired
    private StudentService studentService;

    @ApiOperation("获取指定id记录")
    @RequestMapping(path = "/get", method = RequestMethod.GET)
    public Result<Student> getLimitOne(@RequestParam String id) {
        Student result = studentService.getLimitOne(id);
        return ResultUtil.success(result);
    }

    /**
     * 获取学生记录
     *
     * @return
     */
    @ApiOperation("获取学生记录")
    @RequestMapping(path = "/listForObject", method = RequestMethod.GET)
    public Result<List<Student>> getListForObject() {
        List<Student> result = studentService.getListForObject();
        return ResultUtil.success(result);
    }

    /**
     * 获取学生爱好记录
     *
     * @return
     */
    @ApiOperation("获取学生爱好记录")
    @RequestMapping(path = "/listStudentHobbyForObject", method = RequestMethod.GET)
    public Result<List<StudentHobby>> getListStudentHobby() {
        List<StudentHobby> result = studentService.getListStudentHobby();
        return ResultUtil.success(result);
    }
}
