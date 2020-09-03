package com.hadoop.hive.controller;

import com.hadoop.hive.common.Result;
import com.hadoop.hive.common.ResultUtil;
import com.hadoop.hive.entity.Student;
import com.hadoop.hive.repository.HiveRepository;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

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
     * 获取表全部记录
     *
     * @param sql select id,name,score,age from student
     * @return
     */
    @ApiOperation("获取表全部记录")
    @RequestMapping(path = "/listForObject", method = RequestMethod.POST)
    public Result<List<Student>> getListForObject(@RequestParam String sql) {
        List<Student> result = hiveRepository.getListForObject(sql);
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
     * 加载文件数据到表
     *
     * @return
     */
    @ApiOperation("加载文件数据到表")
    @RequestMapping(path = "/loadIntoTable", method = RequestMethod.GET)
    public Result<String> loadIntoTable() {
        String filePath = "/home/hive_data/student_sample.txt";
        String tableName = "student_sample";

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
}
