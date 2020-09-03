package com.hadoop.hive.controller;

import com.hadoop.hive.common.Result;
import com.hadoop.hive.common.ResultUtil;
import com.hadoop.hive.entity.Student;
import com.hadoop.hive.repository.HiveRepository;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@Api(tags = "HiveController", description = "Spring Boot 集成Hive操作 Demo")
@RestController
@RequestMapping("/api/v1/hive")
public class HiveController {

    @Autowired
    private HiveRepository hiveRepository;

    @ApiOperation("list")
    @RequestMapping(path = "/list", method = RequestMethod.POST)
    public Result<List<Map<String, Object>>> list(@RequestParam String sql) {
        List<Map<String, Object>> list = hiveRepository.queryForList(sql);

        return ResultUtil.success(list);
    }

    @ApiOperation("get")
    @RequestMapping(path = "/get", method = RequestMethod.GET)
    public Result<Student> getLimitOne(@RequestParam String id) {
        String sql = "select * from student where id ='" + id + "'";
        Student result = hiveRepository.getLimitOne(sql);
        return ResultUtil.success(result);
    }

    @ApiOperation("listForObject")
    @RequestMapping(path = "/listForObject", method = RequestMethod.GET)
    public Result<List<Student>> getListForObject() {
        String sql = "select * from student";
        List<Student> result = hiveRepository.getListForObject(sql);
        return ResultUtil.success(result);
    }

    @ApiOperation("createTable")
    @RequestMapping(path = "/createTable", method = RequestMethod.GET)
    public Result<String> createTable() {

        String sql = "CREATE TABLE IF NOT EXISTS " + "student_sample" +
                "(user_num BIGINT, user_name STRING, user_gender STRING, user_age INT)" +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' " + // 定义分隔符
                "STORED AS TEXTFILE";// 作为文本存储
        String result = hiveRepository.createTable(sql);

        return ResultUtil.success(result);
    }

    @ApiOperation("loadIntoTable")
    @RequestMapping(path = "/loadIntoTable", method = RequestMethod.GET)
    public Result<String> loadIntoTable() {
        String filePath = "/home/hive_data/student_sample.txt";
        String tableName = "student_sample";

        String result = hiveRepository.loadIntoTable(filePath, tableName);
        return ResultUtil.success(result);
    }

    @ApiOperation("listAllTables")
    @RequestMapping(path = "/listAllTables", method = RequestMethod.GET)
    public Result<List<String>> listAllTables() {
        try {
            List<String> results = hiveRepository.listAllTables();
            return ResultUtil.success(results);
        } catch (Exception ex) {
            return ResultUtil.failure(ex.getMessage());
        }
    }

    @ApiOperation("describeTable")
    @RequestMapping(path = "/describeTable", method = RequestMethod.GET)
    public Result<List<String>> describeTable() {
        String tableName = "student_sample";
        try {
            List<String> results = hiveRepository.describeTable(tableName);
            return ResultUtil.success(results);
        } catch (Exception ex) {
            return ResultUtil.failure(ex.getMessage());
        }
    }
}
