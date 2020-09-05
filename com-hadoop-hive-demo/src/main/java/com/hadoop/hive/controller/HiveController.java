package com.hadoop.hive.controller;

import com.hadoop.hive.common.Result;
import com.hadoop.hive.common.ResultUtil;
import com.hadoop.hive.entity.Employee;
import com.hadoop.hive.entity.EmployeeComplexStructure;
import com.hadoop.hive.entity.Student;
import com.hadoop.hive.entity.StudentHobby;
import com.hadoop.hive.entity.database.TableInfo;
import com.hadoop.hive.repository.HiveRepository;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api(tags = "HiveController", description = "Spring Boot 集成Hive操作 Demo")
@RestController
@RequestMapping("/api/v1/hive")
public class HiveController {

    @Autowired
    private HiveRepository hiveRepository;

    @ApiOperation("执行SQL获取数据记录")
    @RequestMapping(path = "/list", method = RequestMethod.POST)
    public Result<List<Map<String, Object>>> list(@RequestParam String sql) {
        List<Map<String, Object>> list = hiveRepository.queryForList(sql);

        return ResultUtil.success(list);
    }

    @ApiOperation("获取指定id记录")
    @RequestMapping(path = "/get", method = RequestMethod.GET)
    public Result<Student> getLimitOne(@RequestParam String id) {
        String sql = "select id,name,score,age from student where id ='" + id + "'";
        Student result = hiveRepository.getLimitOne(sql);
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
        String sql = "select id,name,score,age from student";
        List<Student> result = hiveRepository.getListForObject(sql);
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
        String sql = "select id,name,hobby,add from student";
        List<StudentHobby> result = hiveRepository.getListStudentHobby(sql);
        return ResultUtil.success(result);
    }

    @ApiOperation("获取员工记录")
    @RequestMapping(path = "/listEmployeeForObject", method = RequestMethod.GET)
    public Result<List<Employee>> getListEmployee() {
        String sql = "select id,info from employee";
        List<Employee> result = hiveRepository.getListEmployee(sql);
        return ResultUtil.success(result);
    }

    @ApiOperation("获取复杂员工记录")
    @RequestMapping(path = "/listEmployeeComplexStructureForObject", method = RequestMethod.GET)
    public Result<List<EmployeeComplexStructure>> getListEmployeeComplexStructure() {
        String sql = "select name,sa1ary,subordinates,deductions,address from employee_complex_structure";
        List<EmployeeComplexStructure> result = hiveRepository.getListEmployeeComplexStructure(sql);
        return ResultUtil.success(result);
    }

    @ApiOperation("查询指定条件复杂员工记录")
    @RequestMapping(path = "/listEmployeeComplexStructureForObjectByParam", method = RequestMethod.POST)
    public Result<List<EmployeeComplexStructure>> getListEmployeeComplexStructureByParam(String name, String city) {
        String sql = "select name,sa1ary,subordinates,deductions,address from employee_complex_structure where 1=1 ";
        List<EmployeeComplexStructure> result = hiveRepository.getListEmployeeComplexStructureByParam(sql, name, city);
        return ResultUtil.success(result);
    }

    /**
     * 创建表
     *
     * @param sql CREATE TABLE IF NOT EXISTS student_sample(user_num BIGINT, user_name STRING, user_gender STRING, user_age INT)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
     * @return
     */
    @ApiOperation("创建表")
    @RequestMapping(path = "/createTable", method = RequestMethod.POST)
    public Result<String> createTable(@RequestParam String sql) {
        String result = hiveRepository.createTable(sql);
        return ResultUtil.success(result);
    }

    /**
     * 从本地数据加载进入hive
     *
     * @param filePath  "/home/hive_data/student_sample.txt"
     * @param tableName "student_sample";
     * @return
     */
    @ApiOperation("从本地数据加载进入hive")
    @RequestMapping(path = "/loadIntoTable", method = RequestMethod.GET)
    public Result<String> loadIntoTable(String filePath, String tableName) {
        String result = hiveRepository.loadIntoTable(filePath, tableName);
        return ResultUtil.success(result);
    }

    @ApiOperation("获取所有表")
    @RequestMapping(path = "/listAllTables", method = RequestMethod.GET)
    public Result<List<String>> listAllTables() {
        try {
            List<String> results = hiveRepository.listAllTables();
            return ResultUtil.success(results);
        } catch (Exception ex) {
            return ResultUtil.failure(ex.getMessage());
        }
    }

    @ApiOperation("获取表结构")
    @RequestMapping(path = "/describeTable/{tableName}", method = RequestMethod.GET)
    public Result<List<String>> describeTable(@PathVariable String tableName) {
        try {
            List<String> results = hiveRepository.describeTable(tableName);
            return ResultUtil.success(results);
        } catch (Exception ex) {
            return ResultUtil.failure(ex.getMessage());
        }
    }

    @ApiOperation("获取表结构详细信息")
    @RequestMapping(path = "/describeTableInfo/{tableName}", method = RequestMethod.GET)
    public Result<List<TableInfo>> describeTableInfo(@PathVariable String tableName) {
        try {
            List<TableInfo> results = hiveRepository.describeTableInfo(tableName);
            return ResultUtil.success(results);
        } catch (Exception ex) {
            return ResultUtil.failure(ex.getMessage());
        }
    }
}
