package com.hadoop.hive.controller;

import com.hadoop.hive.common.Result;
import com.hadoop.hive.common.ResultUtil;
import com.hadoop.hive.entity.database.TableInfo;
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
